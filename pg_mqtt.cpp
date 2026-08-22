/*
 * pg_mqtt.cpp
 *
 * Phase 1: pg_mqtt_link_check() - proof-of-linkage only, see README.
 *
 * Phase 2: mqtt_publish_binary/text/json/jsonb(topic, payload, qos, retain)
 * - publishes a message to an MQTT broker. One boost::mqtt5::mqtt_client
 * per backend, lazily started and kept alive for the backend's lifetime
 * (mirrors pg_blazingmq.cpp's get_session() pattern exactly). Unlike
 * BlazingMQ's bmqa::Session, Boost.MQTT5's mqtt_client is a raw Asio
 * object - nothing happens unless its io_context is pumped continuously,
 * so a dedicated background thread runs ioc.run() for the client's whole
 * lifetime; async_run() itself is a long-lived operation (only completes
 * on disconnect/cancel/fatal error), kicked off once via a detached
 * completion token. Each SQL-level publish call is a synchronous wrapper:
 * asio::post() the actual async_publish call onto that io_context, block
 * the calling (backend) thread on a std::promise/future until the
 * completion handler (which runs on the io_context thread) resolves it.
 *
 * Phase 3: mqtt_subscribe(topic, callback_fn, qos)/mqtt_unsubscribe(pid) -
 * push-consume via a dynamic background worker, mirroring pg_blazingmq.cpp's
 * bmq_subscribe/bmq_unsubscribe/bmq_subscriber_main architecture directly
 * (DSM-based one-shot config handoff, an atomic readiness flag closing the
 * race between "worker process started" and "worker's subscription is
 * actually open", PID-as-handle with pg_stat_activity.backend_type as the
 * registry). One real, honest difference from bmq_subscribe: Boost.MQTT5's
 * async_receive() has no manual-ack parameter anywhere in its API - PUBACK/
 * PUBREC/PUBREL/PUBCOMP handshaking happens inside the client library
 * itself, independent of whether the SQL callback ever runs or succeeds.
 * So unlike BlazingMQ's push-consume (true at-least-once - a failed
 * callback leaves the message unconfirmed for the broker to redeliver),
 * pg_mqtt's subscribe path only offers "QoS guarantees delivery to the
 * client", not "to a successfully-completed callback" - a callback error
 * here just logs a WARNING and moves on; the message is not, and cannot be,
 * redelivered because of it.
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "storage/ipc.h"
#include "varatt.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "port/atomics.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include <signal.h>
#include <unistd.h>

PG_MODULE_MAGIC;

void _PG_init(void);
PGDLLEXPORT void mqtt_subscriber_main(Datum main_arg);
}

#include <boost/mqtt5.hpp>
#include <boost/mqtt5/ssl.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace mqtt5 = boost::mqtt5;
namespace asio = boost::asio;
using tcp_client = mqtt5::mqtt_client<asio::ip::tcp::socket>;
using tls_stream = asio::ssl::stream<asio::ip::tcp::socket>;
using tls_client = mqtt5::mqtt_client<tls_stream, asio::ssl::context>;

// Boost.MQTT5's tls_handshake_type<StreamType> trait (async_traits.hpp) is
// an empty primary template that the caller is expected to specialize per
// TLS stream type - <boost/mqtt5/ssl.hpp> only provides async_shutdown()
// for asio::ssl::stream, not this. Without it, connect_op.hpp's
// tls_handshake_type<Stream>::client fails to compile the moment a
// TLS-capable mqtt_client is actually instantiated.
namespace boost::mqtt5 {
template <typename Stream>
struct tls_handshake_type<asio::ssl::stream<Stream>> {
    static constexpr auto client = asio::ssl::stream_base::client;
    static constexpr auto server = asio::ssl::stream_base::server;
};

// Likewise: async_traits.hpp declares assign_tls_sni<TlsContext, TlsStream>
// (used to set the TLS SNI extension from the broker hostname before the
// handshake) but leaves it undefined for the caller to specialize - a
// missing specialization is a link error (undefined symbol), not a
// compile error, since it's only referenced, never instantiated with a
// body, until a TLS-capable mqtt_client is actually used.
template <>
inline void assign_tls_sni<asio::ssl::context, tls_stream>(
    const authority_path& ap, asio::ssl::context& /*ctx*/, tls_stream& s)
{
    SSL_set_tlsext_host_name(s.native_handle(), ap.host.c_str());
}
}

// --- GUC ---------------------------------------------------------------

static char* g_broker_host = nullptr;
static int g_broker_port = 1883;
static char* g_client_id = nullptr;
static char* g_broker_username = nullptr;
static char* g_broker_password = nullptr;
static char* g_will_topic = nullptr;
static char* g_will_payload = nullptr;
static int g_will_qos = 0;
static bool g_will_retain = false;
static int g_session_expiry_seconds = 0;
static bool g_tls_enabled = false;
static char* g_tls_ca_file = nullptr;
static char* g_tls_cert_file = nullptr;
static char* g_tls_key_file = nullptr;

void _PG_init(void)
{
    DefineCustomStringVariable(
        "pg_mqtt.broker_host",
        "MQTT broker host used by pg_mqtt functions.",
        NULL,
        &g_broker_host,
        "localhost",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_mqtt.broker_port",
        "MQTT broker port used by pg_mqtt functions.",
        NULL,
        &g_broker_port,
        1883,
        1, 65535,
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.client_id",
        "MQTT Client Identifier used when connecting to the broker. Empty "
        "(the default) lets the broker assign one, which changes on every "
        "reconnect - set this explicitly if pg_mqtt.session_expiry_seconds "
        "is used, since session resumption is keyed by Client ID and a "
        "broker-assigned one defeats it.",
        NULL,
        &g_client_id,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.broker_username",
        "Username for MQTT broker authentication. Empty (the default) "
        "connects without credentials.",
        NULL,
        &g_broker_username,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.broker_password",
        "Password for MQTT broker authentication, used together with "
        "pg_mqtt.broker_username.",
        NULL,
        &g_broker_password,
        "",
        PGC_SUSET,
        GUC_SUPERUSER_ONLY,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.will_topic",
        "Last Will and Testament topic. Empty (the default) means no Will "
        "Message is configured. The broker publishes the Will Message if "
        "this connection closes without a clean disconnect.",
        NULL,
        &g_will_topic,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.will_payload",
        "Last Will and Testament payload, published verbatim as the Will "
        "Message's body. Only used if pg_mqtt.will_topic is set.",
        NULL,
        &g_will_payload,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_mqtt.will_qos",
        "QoS level (0, 1, or 2) used when the broker publishes the Will "
        "Message.",
        NULL,
        &g_will_qos,
        0,
        0, 2,
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "pg_mqtt.will_retain",
        "Whether the Will Message is published with the retain flag set.",
        NULL,
        &g_will_retain,
        false,
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_mqtt.session_expiry_seconds",
        "MQTT 5 Session Expiry Interval sent at CONNECT time, in seconds. "
        "0 (the default) means no session-persistence hint is sent (today's "
        "prior behavior, unchanged). Boost.MQTT5 does not expose a separate "
        "Clean Start flag - this is the only session-persistence knob "
        "available through this client library. Requires pg_mqtt.client_id "
        "to be set to a stable value to actually mean anything across "
        "reconnects.",
        NULL,
        &g_session_expiry_seconds,
        0,
        0, 604800,
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "pg_mqtt.tls_enabled",
        "Connect to the broker over TLS instead of a plain TCP socket.",
        NULL,
        &g_tls_enabled,
        false,
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.tls_ca_file",
        "CA certificate file used to verify the broker's TLS certificate. "
        "Empty (the default) uses the system's default CA trust store.",
        NULL,
        &g_tls_ca_file,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.tls_cert_file",
        "Client certificate file for mutual TLS. Empty (the default) means "
        "no client certificate is presented.",
        NULL,
        &g_tls_cert_file,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_mqtt.tls_key_file",
        "Private key file matching pg_mqtt.tls_cert_file, for mutual TLS.",
        NULL,
        &g_tls_key_file,
        "",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);
}

// --- Shared connect-time setup (auth / LWT / session persistence / TLS) -
//
// Applied identically to the backend-side publish client and the
// subscriber worker's client, but each reads its *own* copy of these
// settings: the backend reads the live pg_mqtt.* GUCs directly (which
// reflect any session-level SET override), while the worker gets its copy
// via SubscriberConfig at mqtt_subscribe() time - a background worker is a
// fresh backend process and does not inherit its launching session's
// SET-level GUC overrides, exactly the same reason broker_host/port were
// already threaded through the DSM segment before any of this was added.
// ConnectConfig/TlsConfig exist so both call sites share one
// apply_connect_time_config()/build_tls_context() instead of duplicating
// the Boost.MQTT5 calls themselves.

struct ConnectConfig {
    std::string client_id;
    std::string username;
    std::string password;
    std::optional<mqtt5::will> will;
    uint32_t session_expiry_seconds = 0; // 0 = unset, no hint sent
};

static ConnectConfig connect_config_from_guc()
{
    ConnectConfig cc;
    cc.client_id = g_client_id ? g_client_id : "";
    cc.username = g_broker_username ? g_broker_username : "";
    cc.password = g_broker_password ? g_broker_password : "";
    if (g_will_topic && g_will_topic[0] != '\0') {
        mqtt5::qos_e qos = g_will_qos == 0 ? mqtt5::qos_e::at_most_once
                          : g_will_qos == 1 ? mqtt5::qos_e::at_least_once
                                            : mqtt5::qos_e::exactly_once;
        mqtt5::retain_e retain = g_will_retain ? mqtt5::retain_e::yes : mqtt5::retain_e::no;
        cc.will = mqtt5::will(std::string(g_will_topic),
                               std::string(g_will_payload ? g_will_payload : ""),
                               qos, retain);
    }
    if (g_session_expiry_seconds > 0) {
        cc.session_expiry_seconds = static_cast<uint32_t>(g_session_expiry_seconds);
    }
    return cc;
}

template <typename ClientT>
static void apply_connect_time_config(ClientT& client, const ConnectConfig& cc)
{
    if (!cc.client_id.empty() || !cc.username.empty()) {
        client.credentials(cc.client_id, cc.username, cc.password);
    }
    if (cc.will) client.will(*cc.will);
    if (cc.session_expiry_seconds > 0) {
        mqtt5::connect_props props;
        props[mqtt5::prop::session_expiry_interval] = cc.session_expiry_seconds;
        client.connect_properties(std::move(props));
    }
}

struct TlsConfig {
    bool enabled = false;
    std::string ca_file;
    std::string cert_file;
    std::string key_file;
};

static TlsConfig tls_config_from_guc()
{
    TlsConfig tc;
    tc.enabled = g_tls_enabled;
    tc.ca_file = g_tls_ca_file ? g_tls_ca_file : "";
    tc.cert_file = g_tls_cert_file ? g_tls_cert_file : "";
    tc.key_file = g_tls_key_file ? g_tls_key_file : "";
    return tc;
}

// Builds and configures the TLS context from a TlsConfig. Verification is
// enabled whenever TLS is on: either against an explicit CA file, or the
// system default trust store if none is given.
static asio::ssl::context build_tls_context(const TlsConfig& tc)
{
    asio::ssl::context ctx(asio::ssl::context::tls_client);
    ctx.set_verify_mode(asio::ssl::verify_peer);
    if (!tc.ca_file.empty()) {
        ctx.load_verify_file(tc.ca_file);
    } else {
        ctx.set_default_verify_paths();
    }
    if (!tc.cert_file.empty()) {
        ctx.use_certificate_file(tc.cert_file, asio::ssl::context::pem);
    }
    if (!tc.key_file.empty()) {
        ctx.use_private_key_file(tc.key_file, asio::ssl::context::pem);
    }
    return ctx;
}

// --- Session lifecycle ---------------------------------------------------
//
// One client per backend process, lazily started on first use and torn
// down via on_proc_exit - same shape as pg_blazingmq.cpp's get_session().
// The io_context is pumped by a dedicated background thread for the
// client's whole lifetime (nothing in Boost.MQTT5 happens otherwise);
// async_publish calls are posted onto that thread and the calling
// (backend) thread blocks on a promise/future for the result.

static asio::io_context* g_ioc = nullptr;
static tcp_client* g_client = nullptr;
static tls_client* g_tls_client = nullptr;
static std::thread* g_io_thread = nullptr;
static asio::executor_work_guard<asio::io_context::executor_type>* g_work_guard = nullptr;
static bool g_exit_hook_registered = false;

static void pg_mqtt_exit_hook(int /*code*/, Datum /*arg*/)
{
    if (g_client) {
        g_client->async_disconnect(asio::detached);
    }
    if (g_tls_client) {
        g_tls_client->async_disconnect(asio::detached);
    }
    if (g_work_guard) {
        g_work_guard->reset();
        delete g_work_guard;
        g_work_guard = nullptr;
    }
    if (g_ioc) {
        g_ioc->stop();
    }
    if (g_io_thread) {
        if (g_io_thread->joinable()) g_io_thread->join();
        delete g_io_thread;
        g_io_thread = nullptr;
    }
    delete g_client;
    g_client = nullptr;
    delete g_tls_client;
    g_tls_client = nullptr;
    delete g_ioc;
    g_ioc = nullptr;
}

// pg_mqtt.tls_enabled is only consulted the first time a client is needed
// per backend (like broker_host/port already were) - changing it mid-session
// has no effect on an already-started connection, same as the other
// connect-time GUCs.
static void ensure_client_started()
{
    if (g_client || g_tls_client) return;

    g_ioc = new asio::io_context();
    g_work_guard = new asio::executor_work_guard<asio::io_context::executor_type>(
        asio::make_work_guard(*g_ioc));

    ConnectConfig cc = connect_config_from_guc();
    if (g_tls_enabled) {
        g_tls_client = new tls_client(*g_ioc, build_tls_context(tls_config_from_guc()));
        g_tls_client->brokers(g_broker_host, static_cast<uint16_t>(g_broker_port));
        apply_connect_time_config(*g_tls_client, cc);
    } else {
        g_client = new tcp_client(*g_ioc);
        g_client->brokers(g_broker_host, static_cast<uint16_t>(g_broker_port));
        apply_connect_time_config(*g_client, cc);
    }

    g_io_thread = new std::thread([]() { g_ioc->run(); });

    // Long-lived: only completes on disconnect/cancel/fatal error, so it
    // must be fire-and-forget, not awaited here.
    if (g_tls_client) g_tls_client->async_run(asio::detached);
    else g_client->async_run(asio::detached);

    if (!g_exit_hook_registered) {
        on_proc_exit(pg_mqtt_exit_hook, 0);
        g_exit_hook_registered = true;
    }
}

// --- Publish ---------------------------------------------------------------
//
// QoS is a compile-time template parameter on Boost.MQTT5's async_publish,
// not a runtime one, and QoS 0 vs QoS 1/2 completion handlers have
// different signatures (error_code only, vs error_code + reason_code +
// props) - a generic lambda handles both uniformly, since only the error
// code is used for this first cut. The runtime qos int only selects which
// async_publish<qos_e> template instantiation to call. ClientT is likewise
// a compile-time choice between the plain and TLS client types; which one
// is actually live is decided once per backend by ensure_client_started().

template <typename ClientT>
static void publish_sync_on(ClientT& client, const std::string& topic,
                             const std::string& payload, int qos,
                             const mqtt5::retain_e retain_flag,
                             const mqtt5::publish_props& props)
{
    // Heap-allocated (not a stack-local std::promise) and captured by value
    // in handler: if the bounded wait below is interrupted by
    // CHECK_FOR_INTERRUPTS() unwinding this function's stack via
    // ereport(ERROR), the posted lambda on the io_context thread can still
    // fire later (e.g. once a slow/failing connection attempt finally
    // resolves) and must not write into a promise object that no longer
    // exists.
    auto prom = std::make_shared<std::promise<boost::system::error_code>>();
    std::future<boost::system::error_code> fut = prom->get_future();

    auto handler = [prom](boost::system::error_code ec, auto&&...) {
        prom->set_value(ec);
    };

    if (qos == 0) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, props, handler]() mutable {
            client.template async_publish<mqtt5::qos_e::at_most_once>(
                topic, payload, retain_flag, props, std::move(handler));
        });
    } else if (qos == 1) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, props, handler]() mutable {
            client.template async_publish<mqtt5::qos_e::at_least_once>(
                topic, payload, retain_flag, props, std::move(handler));
        });
    } else if (qos == 2) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, props, handler]() mutable {
            client.template async_publish<mqtt5::qos_e::exactly_once>(
                topic, payload, retain_flag, props, std::move(handler));
        });
    } else {
        ereport(ERROR, (errmsg("qos must be 0, 1, or 2 (got %d)", qos)));
    }

    // Bounded, interruptible wait rather than a raw fut.get(): a publish
    // against a broker that never completes the connection (wrong
    // credentials, unreachable TLS listener, etc. - all newly reachable
    // failure modes once auth/TLS were wired in) would otherwise hang
    // forever with no Postgres-visible interrupt point at all - found live
    // during this session's own verification of the auth feature: the
    // stuck backend didn't even respond to pg_terminate_backend()'s
    // SIGTERM, only SIGKILL. CHECK_FOR_INTERRUPTS() here lets a normal
    // query cancel or backend termination actually take effect.
    while (fut.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
        CHECK_FOR_INTERRUPTS();
    }
    boost::system::error_code ec = fut.get();
    if (ec) {
        ereport(ERROR,
                (errmsg("mqtt publish failed for topic '%s'", topic.c_str()),
                 errdetail("%s", ec.message().c_str())));
    }
}

static void publish_sync(const std::string& topic, const std::string& payload,
                          int qos, bool retain,
                          std::optional<int32_t> message_expiry_seconds,
                          const std::vector<std::pair<std::string, std::string>>& user_props)
{
    ensure_client_started();

    mqtt5::retain_e retain_flag = retain ? mqtt5::retain_e::yes : mqtt5::retain_e::no;
    mqtt5::publish_props props;
    if (message_expiry_seconds) {
        props[mqtt5::prop::message_expiry_interval] =
            static_cast<uint32_t>(*message_expiry_seconds);
    }
    if (!user_props.empty()) {
        props[mqtt5::prop::user_property] = user_props;
    }

    if (g_tls_client) {
        publish_sync_on(*g_tls_client, topic, payload, qos, retain_flag, props);
    } else {
        publish_sync_on(*g_client, topic, payload, qos, retain_flag, props);
    }
}

// Reads a flat JSON object of string keys to string values into
// publish_props::user_property's vector-of-pairs shape. A null jsonb_arg
// yields an empty vector (no User Properties). Any non-object top-level
// value, or any non-string value, is a hard error rather than a silent
// best-effort conversion - MQTT 5 User Properties are UTF-8 string pairs,
// there's no lossless way to coerce a number/bool/array/object into that.
static std::vector<std::pair<std::string, std::string>>
parse_user_properties_jsonb(Jsonb* jb)
{
    std::vector<std::pair<std::string, std::string>> result;
    if (!jb) return result;
    if (!JB_ROOT_IS_OBJECT(jb)) {
        ereport(ERROR,
                (errmsg("user_properties must be a JSON object of string keys "
                        "to string values")));
    }

    JsonbIterator* it = JsonbIteratorInit(&jb->root);
    JsonbIteratorToken tok;
    JsonbValue v;
    std::string pending_key;
    bool have_key = false;

    while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE) {
        if (tok == WJB_KEY) {
            pending_key.assign(v.val.string.val, v.val.string.len);
            have_key = true;
        } else if (tok == WJB_VALUE && have_key) {
            if (v.type != jbvString) {
                ereport(ERROR,
                        (errmsg("user_properties value for key '%s' must be a "
                                "string", pending_key.c_str())));
            }
            result.emplace_back(pending_key,
                                 std::string(v.val.string.val, v.val.string.len));
            have_key = false;
        }
    }
    return result;
}

extern "C" {

PG_FUNCTION_INFO_V1(pg_mqtt_link_check);

Datum pg_mqtt_link_check(PG_FUNCTION_ARGS)
{
    text* host_text = PG_GETARG_TEXT_PP(0);
    std::string host(VARDATA_ANY(host_text), VARSIZE_ANY_EXHDR(host_text));
    int32 port = PG_GETARG_INT32(1);

    if (port <= 0 || port > 65535) {
        ereport(ERROR, (errmsg("port must be between 1 and 65535")));
    }

    try {
        asio::io_context ioc;
        tcp_client client(ioc);
        client.brokers(host, static_cast<uint16_t>(port));

        std::ostringstream out;
        out << "pg_mqtt link OK: broker=" << host << ":" << port;
        PG_RETURN_TEXT_P(cstring_to_text(out.str().c_str()));
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("pg_mqtt_link_check failed"),
                 errdetail("%s", ex.what())));
    }

    PG_RETURN_NULL(); // unreachable
}

PG_FUNCTION_INFO_V1(mqtt_publish_binary);

Datum mqtt_publish_binary(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("topic must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("payload must not be null")));

    text* topic_text = PG_GETARG_TEXT_PP(0);
    std::string topic(VARDATA_ANY(topic_text), VARSIZE_ANY_EXHDR(topic_text));

    bytea* payload_bytea = PG_GETARG_BYTEA_PP(1);
    std::string payload(VARDATA_ANY(payload_bytea), VARSIZE_ANY_EXHDR(payload_bytea));

    int32 qos = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    bool retain = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    std::optional<int32_t> msg_expiry = PG_ARGISNULL(4)
        ? std::nullopt : std::make_optional(PG_GETARG_INT32(4));
    std::vector<std::pair<std::string, std::string>> user_props = PG_ARGISNULL(5)
        ? std::vector<std::pair<std::string, std::string>>{}
        : parse_user_properties_jsonb(PG_GETARG_JSONB_P(5));

    try {
        publish_sync(topic, payload, qos, retain, msg_expiry, user_props);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("mqtt_publish_binary failed"), errdetail("%s", ex.what())));
    }

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(mqtt_publish_text);

Datum mqtt_publish_text(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("topic must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("payload must not be null")));

    text* topic_text = PG_GETARG_TEXT_PP(0);
    std::string topic(VARDATA_ANY(topic_text), VARSIZE_ANY_EXHDR(topic_text));

    text* payload_text = PG_GETARG_TEXT_PP(1);
    std::string payload(VARDATA_ANY(payload_text), VARSIZE_ANY_EXHDR(payload_text));

    int32 qos = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    bool retain = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    std::optional<int32_t> msg_expiry = PG_ARGISNULL(4)
        ? std::nullopt : std::make_optional(PG_GETARG_INT32(4));
    std::vector<std::pair<std::string, std::string>> user_props = PG_ARGISNULL(5)
        ? std::vector<std::pair<std::string, std::string>>{}
        : parse_user_properties_jsonb(PG_GETARG_JSONB_P(5));

    try {
        publish_sync(topic, payload, qos, retain, msg_expiry, user_props);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("mqtt_publish_text failed"), errdetail("%s", ex.what())));
    }

    PG_RETURN_VOID();
}

// json is stored internally as text, so its varlena content is already
// the JSON text representation - no output-function call needed.
PG_FUNCTION_INFO_V1(mqtt_publish_json);

Datum mqtt_publish_json(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("topic must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("payload must not be null")));

    text* topic_text = PG_GETARG_TEXT_PP(0);
    std::string topic(VARDATA_ANY(topic_text), VARSIZE_ANY_EXHDR(topic_text));

    text* payload_text = PG_GETARG_TEXT_PP(1);
    std::string payload(VARDATA_ANY(payload_text), VARSIZE_ANY_EXHDR(payload_text));

    int32 qos = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    bool retain = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    std::optional<int32_t> msg_expiry = PG_ARGISNULL(4)
        ? std::nullopt : std::make_optional(PG_GETARG_INT32(4));
    std::vector<std::pair<std::string, std::string>> user_props = PG_ARGISNULL(5)
        ? std::vector<std::pair<std::string, std::string>>{}
        : parse_user_properties_jsonb(PG_GETARG_JSONB_P(5));

    try {
        publish_sync(topic, payload, qos, retain, msg_expiry, user_props);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("mqtt_publish_json failed"), errdetail("%s", ex.what())));
    }

    PG_RETURN_VOID();
}

// jsonb is a binary format internally - needs jsonb_out to get real JSON text.
PG_FUNCTION_INFO_V1(mqtt_publish_jsonb);

Datum mqtt_publish_jsonb(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("topic must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("payload must not be null")));

    text* topic_text = PG_GETARG_TEXT_PP(0);
    std::string topic(VARDATA_ANY(topic_text), VARSIZE_ANY_EXHDR(topic_text));

    char* payload_cstr = DatumGetCString(
        DirectFunctionCall1(jsonb_out, PG_GETARG_DATUM(1)));
    std::string payload(payload_cstr);

    int32 qos = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
    bool retain = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
    std::optional<int32_t> msg_expiry = PG_ARGISNULL(4)
        ? std::nullopt : std::make_optional(PG_GETARG_INT32(4));
    std::vector<std::pair<std::string, std::string>> user_props = PG_ARGISNULL(5)
        ? std::vector<std::pair<std::string, std::string>>{}
        : parse_user_properties_jsonb(PG_GETARG_JSONB_P(5));

    try {
        publish_sync(topic, payload, qos, retain, msg_expiry, user_props);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("mqtt_publish_jsonb failed"), errdetail("%s", ex.what())));
    }

    PG_RETURN_VOID();
}

} // extern "C" (Phase 1-2 block)

// --- Phase 3: push-consume via a dynamic background worker -----------------
//
// See the header comment for the honest ack-semantics difference from
// pg_blazingmq's bmq_subscribe. Otherwise this mirrors bmq_subscribe's
// architecture directly: a DSM segment hands off one-shot config to a
// freshly registered dynamic background worker (not a shmem_request_hook
// structure, which would need shared_preload_libraries + a restart); the
// worker's PID doubles as the subscription handle via pg_stat_activity.

struct SubscriberConfig {
    Oid dbid;
    Oid roleid;
    Oid callback_fn;
    char broker_host[256];
    int broker_port;
    char topic[512];
    int qos;
    // Connect-time settings, copied from the calling backend's live GUCs
    // at mqtt_subscribe() time - a background worker is a fresh process
    // and does not inherit session-level SET overrides, same reason
    // broker_host/port above are copied rather than re-read from the GUC
    // in the worker. Empty client_id/username means "unset", same as the
    // GUC defaults.
    char client_id[256];
    char username[256];
    char password[256];
    char will_topic[512];
    char will_payload[1024];
    int will_qos;
    bool will_retain;
    int session_expiry_seconds;
    bool tls_enabled;
    char tls_ca_file[512];
    char tls_cert_file[512];
    char tls_key_file[512];
    // See bmq_subscribe's identical field for the reasoning:
    // WaitForBackgroundWorkerStartup() only proves the OS process started,
    // not that async_subscribe() has actually completed. The worker flips
    // this to 1 right after a successful SUBACK; mqtt_subscribe() polls it
    // (bounded) before returning.
    pg_atomic_uint32 ready;
};

static const char* kSubscriberBgwType = "pg_mqtt subscriber";

static void validate_callback_fn(Oid callback_fn)
{
    HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(callback_fn));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errmsg("callback function with OID %u does not exist", callback_fn)));
    }
    Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);
    bool ok = (proc->pronargs == 1) && (proc->proargtypes.values[0] == BYTEAOID);
    ReleaseSysCache(tup);
    if (!ok) {
        ereport(ERROR,
                (errmsg("callback function must take exactly one \"bytea\" argument")));
    }
}

extern "C" {

PG_FUNCTION_INFO_V1(mqtt_subscribe);

Datum mqtt_subscribe(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("topic must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("callback_fn must not be null")));

    text* topic_text = PG_GETARG_TEXT_PP(0);
    std::string topic(VARDATA_ANY(topic_text), VARSIZE_ANY_EXHDR(topic_text));
    Oid callback_fn = PG_GETARG_OID(1);
    int32 qos = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);

    if (qos < 0 || qos > 2) {
        ereport(ERROR, (errmsg("qos must be 0, 1, or 2 (got %d)", qos)));
    }
    validate_callback_fn(callback_fn);

    std::string broker_host(g_broker_host ? g_broker_host : "localhost");
    if (topic.size() >= sizeof(SubscriberConfig::topic)) {
        ereport(ERROR, (errmsg("topic is too long (max %zu bytes)",
                                sizeof(SubscriberConfig::topic) - 1)));
    }
    if (broker_host.size() >= sizeof(SubscriberConfig::broker_host)) {
        ereport(ERROR, (errmsg("broker_host is too long (max %zu bytes)",
                                sizeof(SubscriberConfig::broker_host) - 1)));
    }

    // A subscriber worker connecting with the same fixed Client ID as the
    // backend's own publish client (or another concurrent subscriber) would
    // fight over one MQTT identity - brokers disconnect the older connection
    // on a duplicate Client ID (takeover semantics). If a base client_id is
    // configured, derive a per-topic-stable suffix rather than using it
    // verbatim, so a subscription's identity (and therefore session
    // persistence, if pg_mqtt.session_expiry_seconds is set) survives a
    // worker restart on the same topic. Known limitation: two concurrent
    // mqtt_subscribe() calls on the *same* topic still collide - not solved
    // here, document in README.
    std::string base_client_id(g_client_id ? g_client_id : "");
    std::string sub_client_id = base_client_id.empty()
        ? std::string() : base_client_id + "-sub-" + topic;
    std::string username(g_broker_username ? g_broker_username : "");
    std::string password(g_broker_password ? g_broker_password : "");
    std::string will_topic(g_will_topic ? g_will_topic : "");
    std::string will_payload(g_will_payload ? g_will_payload : "");
    std::string tls_ca_file(g_tls_ca_file ? g_tls_ca_file : "");
    std::string tls_cert_file(g_tls_cert_file ? g_tls_cert_file : "");
    std::string tls_key_file(g_tls_key_file ? g_tls_key_file : "");

#define PG_MQTT_CHECK_FIELD_LEN(str, field) \
    if ((str).size() >= sizeof(SubscriberConfig::field)) { \
        ereport(ERROR, (errmsg(#field " is too long (max %zu bytes)", \
                                sizeof(SubscriberConfig::field) - 1))); \
    }
    PG_MQTT_CHECK_FIELD_LEN(sub_client_id, client_id);
    PG_MQTT_CHECK_FIELD_LEN(username, username);
    PG_MQTT_CHECK_FIELD_LEN(password, password);
    PG_MQTT_CHECK_FIELD_LEN(will_topic, will_topic);
    PG_MQTT_CHECK_FIELD_LEN(will_payload, will_payload);
    PG_MQTT_CHECK_FIELD_LEN(tls_ca_file, tls_ca_file);
    PG_MQTT_CHECK_FIELD_LEN(tls_cert_file, tls_cert_file);
    PG_MQTT_CHECK_FIELD_LEN(tls_key_file, tls_key_file);
#undef PG_MQTT_CHECK_FIELD_LEN

    dsm_segment* seg = dsm_create(sizeof(SubscriberConfig), 0);
    SubscriberConfig* cfg = (SubscriberConfig*) dsm_segment_address(seg);
    cfg->dbid = MyDatabaseId;
    cfg->roleid = GetUserId();
    cfg->callback_fn = callback_fn;
    strcpy(cfg->broker_host, broker_host.c_str());
    cfg->broker_port = g_broker_port;
    strcpy(cfg->topic, topic.c_str());
    cfg->qos = qos;
    strcpy(cfg->client_id, sub_client_id.c_str());
    strcpy(cfg->username, username.c_str());
    strcpy(cfg->password, password.c_str());
    strcpy(cfg->will_topic, will_topic.c_str());
    strcpy(cfg->will_payload, will_payload.c_str());
    cfg->will_qos = g_will_qos;
    cfg->will_retain = g_will_retain;
    cfg->session_expiry_seconds = g_session_expiry_seconds;
    cfg->tls_enabled = g_tls_enabled;
    strcpy(cfg->tls_ca_file, tls_ca_file.c_str());
    strcpy(cfg->tls_cert_file, tls_cert_file.c_str());
    strcpy(cfg->tls_key_file, tls_key_file.c_str());
    pg_atomic_init_u32(&cfg->ready, 0);

    // Outlive this backend - the worker attaches independently and this
    // call returns well before the subscription itself ends.
    dsm_pin_segment(seg);

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", kSubscriberBgwType);
    snprintf(worker.bgw_type, BGW_MAXLEN, "%s", kSubscriberBgwType);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_library_name, MAXPGPATH, "pg_mqtt");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "mqtt_subscriber_main");
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
    worker.bgw_notify_pid = MyProcPid;

    BackgroundWorkerHandle* handle;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        dsm_detach(seg);
        ereport(ERROR,
                (errmsg("failed to register pg_mqtt subscriber background worker "
                        "(max_worker_processes may be exhausted)")));
    }

    pid_t pid;
    BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status != BGWH_STARTED) {
        dsm_detach(seg);
        ereport(ERROR,
                (errmsg("pg_mqtt subscriber background worker failed to start "
                        "(status=%d) - check the server log", (int) status)));
    }

    // Bounded wait for the worker to actually finish async_subscribe() (not
    // just for the OS process to start) - see SubscriberConfig::ready.
    // Best-effort: a slow-to-connect worker still gets its pid back with a
    // WARNING rather than a hard failure, since it keeps retrying/running
    // independently either way.
    const int ready_timeout_ms = 5000;
    const int poll_interval_us = 20000;
    int waited_us = 0;
    while (pg_atomic_read_u32(&cfg->ready) == 0 && waited_us < ready_timeout_ms * 1000) {
        pg_usleep(poll_interval_us);
        waited_us += poll_interval_us;
    }
    if (pg_atomic_read_u32(&cfg->ready) == 0) {
        ereport(WARNING,
                (errmsg("pg_mqtt subscriber worker (pid %d) has not finished subscribing "
                        "after %dms - messages published immediately may not be "
                        "delivered; it is still running and will keep trying", pid, ready_timeout_ms)));
    }

    dsm_detach(seg); // the worker has its own attachment now; pinned, so this is safe
    PG_RETURN_INT32((int32) pid);
}

PG_FUNCTION_INFO_V1(mqtt_unsubscribe);

Datum mqtt_unsubscribe(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("worker_pid must not be null")));
    int32 pid = PG_GETARG_INT32(0);

    bool is_ours = false;
    SPI_connect();
    Oid argtypes[2] = {INT4OID, TEXTOID};
    Datum argvalues[2] = {Int32GetDatum(pid), CStringGetTextDatum(kSubscriberBgwType)};
    int rc = SPI_execute_with_args(
        "SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND backend_type = $2",
        2, argtypes, argvalues, nullptr, true, 1);
    if (rc == SPI_OK_SELECT && SPI_processed > 0) is_ours = true;
    SPI_finish();

    if (!is_ours) {
        ereport(ERROR,
                (errmsg("pid %d is not an active pg_mqtt subscriber worker", pid)));
    }

    if (kill(pid, SIGTERM) != 0) {
        ereport(WARNING, (errmsg("failed to signal worker pid %d: %m", pid)));
        PG_RETURN_BOOL(false);
    }
    PG_RETURN_BOOL(true);
}

} // extern "C" (mqtt_subscribe/mqtt_unsubscribe)

// The subscribe+receive loop body, shared between the plain and TLS
// client types via ClientT so the two connection setups (below, in
// mqtt_subscriber_main) don't require duplicating this whole function.
// A template cannot have C linkage, hence the extern "C" block above is
// closed before this and reopened just before mqtt_subscriber_main, whose
// own declaration at the top of this file does need C linkage.
template <typename ClientT>
static void run_subscriber_loop(
    ClientT& client, asio::io_context& ioc, dsm_segment* seg,
    SubscriberConfig* cfg, bool& seg_detached,
    const std::string& topic, int qos, Oid callback_fn)
{
        client.async_run(asio::detached);

        mqtt5::qos_e max_qos = qos == 0 ? mqtt5::qos_e::at_most_once
                              : qos == 1 ? mqtt5::qos_e::at_least_once
                                         : mqtt5::qos_e::exactly_once;
        // Boost.MQTT5 defaults no_local to yes, but the MQTT 5 spec (3.8.3.1)
        // makes it a Protocol Error to set No Local on a Shared Subscription
        // ("$share/<group>/<filter>") - NanoMQ correctly rejects it
        // (observed: "No local is conflict with shared subscription!").
        // Force it off for shared subscriptions; leave the default alone
        // otherwise since it's a real, useful option for plain topics.
        mqtt5::no_local_e no_local = topic.rfind("$share/", 0) == 0
            ? mqtt5::no_local_e::no : mqtt5::no_local_e::yes;
        mqtt5::subscribe_topic sub_topic{
            topic, mqtt5::subscribe_options{max_qos, no_local}};

        // Single-threaded worker: this same thread both posts the
        // subscribe request and pumps the io_context, so drive run_one()
        // directly until the completion handler below actually fires
        // (signaled via sub_done), rather than blocking on a promise/future
        // the way the backend-side (multi-threaded) publish path does.
        bool sub_done = false;
        boost::system::error_code final_ec;
        client.async_subscribe(
            std::vector<mqtt5::subscribe_topic>{sub_topic}, mqtt5::subscribe_props{},
            [&](boost::system::error_code ec, std::vector<mqtt5::reason_code>, mqtt5::suback_props) {
                final_ec = ec;
                sub_done = true;
            });
        // Bounded poll, not a blocking ioc.run_one(): if the broker is
        // unreachable at startup, async_subscribe's completion handler never
        // fires and an unbounded run_one() blocks in epoll_wait forever,
        // deaf to SIGTERM - mqtt_unsubscribe() would then report success
        // (the signal really was delivered) while this worker lived on
        // indefinitely. Reproduced live via gdb: Thread 1 parked in
        // boost::asio::io_context::run_one() at this exact line, Thread 2
        // (the resolver/connect worker) parked on a condvar wait, neither
        // ever returning control to check ShutdownRequestPending. Mirrors
        // the main receive loop below, which already polls this way.
        while (!sub_done) {
            if (ShutdownRequestPending) {
                throw std::runtime_error(
                    "shutdown requested while waiting for broker subscribe "
                    "to complete (broker likely unreachable)");
            }
            CHECK_FOR_INTERRUPTS();
            ioc.run_one_for(std::chrono::milliseconds(500));
        }
        if (final_ec) {
            throw std::runtime_error("async_subscribe failed: " + final_ec.message());
        }

        pg_atomic_write_u32(&cfg->ready, 1);
        dsm_detach(seg); // caller may now be polling ready=1 and return at any time
        seg_detached = true;

        while (!ShutdownRequestPending) {
            CHECK_FOR_INTERRUPTS();

            bool recv_done = false;
            boost::system::error_code recv_ec;
            std::string recv_topic, recv_payload;
            client.async_receive(
                [&](boost::system::error_code ec, std::string t, std::string p, mqtt5::publish_props) {
                    recv_ec = ec;
                    recv_topic = std::move(t);
                    recv_payload = std::move(p);
                    recv_done = true;
                });

            // Short poll so SIGTERM is noticed promptly - run_one() with a
            // bounded number of iterations, checking ShutdownRequestPending
            // between each, rather than blocking indefinitely on run_one().
            while (!recv_done && !ShutdownRequestPending) {
                ioc.run_one_for(std::chrono::milliseconds(500));
            }
            if (ShutdownRequestPending) break;
            if (recv_ec) continue; // e.g. operation_aborted during shutdown race

            int dataSize = (int) recv_payload.size();
            bytea* payload = (bytea*) palloc(VARHDRSZ + dataSize);
            SET_VARSIZE(payload, VARHDRSZ + dataSize);
            if (dataSize > 0) {
                memcpy(VARDATA(payload), recv_payload.data(), dataSize);
            }

            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());

            bool callback_ok = true;
            PG_TRY();
            {
                OidFunctionCall1(callback_fn, PointerGetDatum(payload));
            }
            PG_CATCH();
            {
                ErrorData* edata = CopyErrorData();
                // Copy the message out to our own std::string *before*
                // FlushErrorState()/AbortCurrentTransaction() run - a bug
                // was reproduced where using edata->message directly in the
                // ereport() below (after the abort) corrupted the heap
                // (pfree on an invalid pointer, with the freed chunk's
                // header overwritten by fragments of this same message
                // text). CopyErrorData() is documented to make edata
                // independent of FlushErrorState(), but empirically its
                // *message buffer* did not survive AbortCurrentTransaction()
                // intact here - copying to a std::string right away avoids
                // relying on edata->message's lifetime past this point.
                std::string err_message(edata->message);
                FreeErrorData(edata);
                FlushErrorState();
                callback_ok = false;
                PopActiveSnapshot();
                SPI_finish();
                AbortCurrentTransaction();
                // No manual-ack API in Boost.MQTT5 - the broker's PUBACK/
                // PUBREC handshake already happened inside async_receive()
                // regardless of this failure, so unlike bmq_subscriber_main
                // this message will *not* be redelivered because of this
                // error. See the header comment for the full explanation.
                ereport(WARNING,
                        (errmsg("pg_mqtt subscriber: callback failed for topic "
                                "'%s' (message already acknowledged to broker, "
                                "will not be redelivered): %s",
                                topic.c_str(), err_message.c_str())));
            }
            PG_END_TRY();

            if (callback_ok) {
                PopActiveSnapshot();
                SPI_finish();
                CommitTransactionCommand();
            }

            if (ShutdownRequestPending) break;
        }
}

extern "C" {

void mqtt_subscriber_main(Datum main_arg)
{
    dsm_segment* seg = dsm_attach(DatumGetUInt32(main_arg));
    if (!seg) {
        ereport(FATAL, (errmsg("pg_mqtt subscriber: failed to attach DSM segment")));
    }
    SubscriberConfig* cfg = (SubscriberConfig*) dsm_segment_address(seg);

    std::string broker_host(cfg->broker_host);
    int broker_port = cfg->broker_port;
    std::string topic(cfg->topic);
    int qos = cfg->qos;
    Oid callback_fn = cfg->callback_fn;
    Oid dbid = cfg->dbid;
    Oid roleid = cfg->roleid;

    ConnectConfig cc;
    cc.client_id = cfg->client_id;
    cc.username = cfg->username;
    cc.password = cfg->password;
    if (cfg->will_topic[0] != '\0') {
        mqtt5::qos_e will_qos = cfg->will_qos == 0 ? mqtt5::qos_e::at_most_once
                               : cfg->will_qos == 1 ? mqtt5::qos_e::at_least_once
                                                     : mqtt5::qos_e::exactly_once;
        mqtt5::retain_e will_retain = cfg->will_retain ? mqtt5::retain_e::yes : mqtt5::retain_e::no;
        cc.will = mqtt5::will(std::string(cfg->will_topic), std::string(cfg->will_payload),
                               will_qos, will_retain);
    }
    if (cfg->session_expiry_seconds > 0) {
        cc.session_expiry_seconds = static_cast<uint32_t>(cfg->session_expiry_seconds);
    }
    TlsConfig tc;
    tc.enabled = cfg->tls_enabled;
    tc.ca_file = cfg->tls_ca_file;
    tc.cert_file = cfg->tls_cert_file;
    tc.key_file = cfg->tls_key_file;

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnectionByOid(dbid, roleid, 0);

    elog(LOG, "pg_mqtt subscriber started for topic '%s'", topic.c_str());

    bool seg_detached = false;
    try {
        asio::io_context ioc;
        if (tc.enabled) {
            tls_client client(ioc, build_tls_context(tc));
            client.brokers(broker_host, static_cast<uint16_t>(broker_port));
            apply_connect_time_config(client, cc);
            run_subscriber_loop(client, ioc, seg, cfg, seg_detached, topic, qos, callback_fn);
        } else {
            tcp_client client(ioc);
            client.brokers(broker_host, static_cast<uint16_t>(broker_port));
            apply_connect_time_config(client, cc);
            run_subscriber_loop(client, ioc, seg, cfg, seg_detached, topic, qos, callback_fn);
        }
    } catch (const std::exception& ex) {
        if (!seg_detached) dsm_detach(seg);
        ereport(LOG,
                (errmsg("pg_mqtt subscriber for topic '%s' exiting on error: %s",
                        topic.c_str(), ex.what())));
    }

    elog(LOG, "pg_mqtt subscriber stopping for topic '%s'", topic.c_str());
    proc_exit(0);
}

} // extern "C" (Phase 3 block)
