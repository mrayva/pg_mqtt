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

    dsm_segment* seg = dsm_create(sizeof(SubscriberConfig), 0);
    SubscriberConfig* cfg = (SubscriberConfig*) dsm_segment_address(seg);
    cfg->dbid = MyDatabaseId;
    cfg->roleid = GetUserId();
    cfg->callback_fn = callback_fn;
    strcpy(cfg->broker_host, broker_host.c_str());
    cfg->broker_port = g_broker_port;
    strcpy(cfg->topic, topic.c_str());
    cfg->qos = qos;
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

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnectionByOid(dbid, roleid, 0);

    elog(LOG, "pg_mqtt subscriber started for topic '%s'", topic.c_str());

    bool seg_detached = false;
    try {
        asio::io_context ioc;
        tcp_client client(ioc);
        client.brokers(broker_host, static_cast<uint16_t>(broker_port));
        client.async_run(asio::detached);

        mqtt5::qos_e max_qos = qos == 0 ? mqtt5::qos_e::at_most_once
                              : qos == 1 ? mqtt5::qos_e::at_least_once
                                         : mqtt5::qos_e::exactly_once;
        mqtt5::subscribe_topic sub_topic{topic, mqtt5::subscribe_options{max_qos}};

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
        while (!sub_done) {
            ioc.run_one();
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
