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
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "storage/ipc.h"
#include "varatt.h"

PG_MODULE_MAGIC;

void _PG_init(void);
}

#include <boost/mqtt5.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <future>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace mqtt5 = boost::mqtt5;
namespace asio = boost::asio;
using tcp_client = mqtt5::mqtt_client<asio::ip::tcp::socket>;

// --- GUC ---------------------------------------------------------------

static char* g_broker_host = nullptr;
static int g_broker_port = 1883;

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
static std::thread* g_io_thread = nullptr;
static asio::executor_work_guard<asio::io_context::executor_type>* g_work_guard = nullptr;
static bool g_exit_hook_registered = false;

static void pg_mqtt_exit_hook(int /*code*/, Datum /*arg*/)
{
    if (g_client) {
        g_client->async_disconnect(asio::detached);
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
    delete g_ioc;
    g_ioc = nullptr;
}

static tcp_client& get_client()
{
    if (g_client) return *g_client;

    g_ioc = new asio::io_context();
    g_work_guard = new asio::executor_work_guard<asio::io_context::executor_type>(
        asio::make_work_guard(*g_ioc));
    g_client = new tcp_client(*g_ioc);
    g_client->brokers(g_broker_host, static_cast<uint16_t>(g_broker_port));

    g_io_thread = new std::thread([]() { g_ioc->run(); });

    // Long-lived: only completes on disconnect/cancel/fatal error, so it
    // must be fire-and-forget, not awaited here.
    g_client->async_run(asio::detached);

    if (!g_exit_hook_registered) {
        on_proc_exit(pg_mqtt_exit_hook, 0);
        g_exit_hook_registered = true;
    }
    return *g_client;
}

// --- Publish ---------------------------------------------------------------
//
// QoS is a compile-time template parameter on Boost.MQTT5's async_publish,
// not a runtime one, and QoS 0 vs QoS 1/2 completion handlers have
// different signatures (error_code only, vs error_code + reason_code +
// props) - a generic lambda handles both uniformly, since only the error
// code is used for this first cut. The runtime qos int only selects which
// async_publish<qos_e> template instantiation to call.

static void publish_sync(const std::string& topic, const std::string& payload,
                          int qos, bool retain)
{
    tcp_client& client = get_client();
    mqtt5::retain_e retain_flag = retain ? mqtt5::retain_e::yes : mqtt5::retain_e::no;

    std::promise<boost::system::error_code> prom;
    std::future<boost::system::error_code> fut = prom.get_future();

    auto handler = [&prom](boost::system::error_code ec, auto&&...) {
        prom.set_value(ec);
    };

    if (qos == 0) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, handler]() mutable {
            client.async_publish<mqtt5::qos_e::at_most_once>(
                topic, payload, retain_flag, mqtt5::publish_props{}, std::move(handler));
        });
    } else if (qos == 1) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, handler]() mutable {
            client.async_publish<mqtt5::qos_e::at_least_once>(
                topic, payload, retain_flag, mqtt5::publish_props{}, std::move(handler));
        });
    } else if (qos == 2) {
        asio::post(*g_ioc, [&client, topic, payload, retain_flag, handler]() mutable {
            client.async_publish<mqtt5::qos_e::exactly_once>(
                topic, payload, retain_flag, mqtt5::publish_props{}, std::move(handler));
        });
    } else {
        ereport(ERROR, (errmsg("qos must be 0, 1, or 2 (got %d)", qos)));
    }

    boost::system::error_code ec = fut.get();
    if (ec) {
        ereport(ERROR,
                (errmsg("mqtt publish failed for topic '%s'", topic.c_str()),
                 errdetail("%s", ec.message().c_str())));
    }
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

    try {
        publish_sync(topic, payload, qos, retain);
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

    try {
        publish_sync(topic, payload, qos, retain);
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

    try {
        publish_sync(topic, payload, qos, retain);
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

    try {
        publish_sync(topic, payload, qos, retain);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("mqtt_publish_jsonb failed"), errdetail("%s", ex.what())));
    }

    PG_RETURN_VOID();
}

} // extern "C"
