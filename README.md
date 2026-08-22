# pg_mqtt

A PostgreSQL extension for publishing/consuming MQTT messages directly from
SQL, in the same spirit as this project's sibling extensions `pg_blazingmq`
(BlazingMQ pub/sub) and `pg_zerialize` (binary row (de)serialization). The
SQL interface is deliberately modeled on `pgnats`'s function shape
(`mqtt_publish_binary`/`text`/`json`/`jsonb`, `mqtt_subscribe`/
`mqtt_unsubscribe`) for consistency across this workspace's messaging
extensions - the implementation is unrelated (C++/PGXS/Boost.MQTT5, not
Rust/pgrx), only the SQL-facing shape is shared.

## Status: Phase 3 (subscribe/push-consume)

`pg_mqtt_link_check(broker_host, broker_port)` constructs a real
`boost::mqtt5::mqtt_client` (without calling `async_run()`, so no live
broker is needed) to prove the extension's `.so` actually links and loads
inside a Postgres backend against the real Boost.MQTT5 dependency chain.

```sql
CREATE EXTENSION pg_mqtt;
SELECT pg_mqtt_link_check('localhost', 1883);
--            pg_mqtt_link_check
-- ----------------------------------------
--  pg_mqtt link OK: broker=localhost:1883
```

`mqtt_publish_binary/text/json/jsonb(topic, payload, qos DEFAULT 0, retain
DEFAULT false)` publish a message. `qos` is MQTT's own delivery guarantee,
not a NATS concept:

- **0** (`at_most_once`) - fire and forget, no acknowledgment.
- **1** (`at_least_once`) - waits for the broker's `PUBACK`.
- **2** (`exactly_once`) - waits for the full four-packet `PUBREC`/
  `PUBREL`/`PUBCOMP` handshake.

`retain`, if true, tells the broker to keep this as the topic's retained
message - any *future* subscriber gets it immediately on connecting, even
if they subscribe long after this call returns (verified directly: a brand
new subscriber, connecting with no publish happening in between, received
a retained message instantly).

```sql
SET pg_mqtt.broker_host = 'localhost';
SET pg_mqtt.broker_port = 1883;

SELECT mqtt_publish_text('sensors/room1/temp', '21.5', 1, false);
SELECT mqtt_publish_jsonb('sensors/room1/status', '{"online": true}'::jsonb, 1, true);
```

One session (`boost::mqtt5::mqtt_client`) per backend, lazily started on
first use and torn down via `on_proc_exit` - same shape as
`pg_blazingmq.cpp`'s `get_session()`. Unlike BlazingMQ's `bmqa::Session`,
Boost.MQTT5's client is a raw Asio object that does nothing unless its
`io_context` is pumped continuously, so a dedicated background thread runs
`ioc.run()` for the client's whole lifetime; `async_run()` itself is a
long-lived operation (only completes on disconnect/cancel/fatal error), so
it's kicked off once, fire-and-forget. Each SQL-level publish is a
synchronous wrapper around that: post the actual `async_publish` call onto
the `io_context`, block the calling backend thread on a
`std::promise`/`future` until the completion handler (running on the
background thread) resolves it. QoS is a *compile-time* template
parameter on Boost.MQTT5's `async_publish<qos_e>` - the runtime `qos int`
from SQL dispatches to one of three template instantiations at each call.

`mqtt_subscribe(topic, callback_fn, qos DEFAULT 0)` registers a dedicated
background worker that stays subscribed to `topic` indefinitely, calling
`callback_fn(payload bytea)` for every message received. Returns the
worker's PID, which doubles as the subscription handle for
`mqtt_unsubscribe(worker_pid)`. Mirrors `pg_blazingmq`'s `bmq_subscribe`
architecture directly: a DSM segment hands off one-shot config to a freshly
registered dynamic background worker, an atomic readiness flag closes the
race between "worker process started" and "worker's subscription is
actually open" (bounded 5s wait, best-effort), and the worker's PID doubles
as the subscription handle via `pg_stat_activity.backend_type` -
`mqtt_unsubscribe()` validates a PID against this before signaling it, so
it can't be used to kill arbitrary processes.

```sql
CREATE TABLE received_messages (topic_hint text, payload text, received_at timestamptz DEFAULT now());

CREATE FUNCTION handle_message(payload bytea) RETURNS void AS $$
BEGIN
  INSERT INTO received_messages (topic_hint, payload) VALUES ('sensors', convert_from(payload, 'UTF8'));
END;
$$ LANGUAGE plpgsql;

SET pg_mqtt.broker_host = 'localhost';
SET pg_mqtt.broker_port = 1883;
SELECT mqtt_subscribe('sensors/+/temp', 'handle_message'::regproc, 1) AS worker_pid;
--  worker_pid
-- ------------
--      901816

-- ... messages arrive asynchronously, with no further action from this session ...

SELECT mqtt_unsubscribe(901816);  -- stops the worker cleanly
```

**Important, honest limitation vs `pg_blazingmq`'s `bmq_subscribe`**: this is
*not* true at-least-once delivery. Boost.MQTT5's client library handles
MQTT's PUBACK/PUBREC/PUBREL/PUBCOMP acknowledgment internally, with no way
to defer or control it from application code (confirmed by reading the
whole `boost/mqtt5` header tree - there is no manual-ack API anywhere). So
the broker considers a message delivered, and won't redeliver it,
regardless of whether `callback_fn` ever runs or succeeds. A failed
callback's transaction is rolled back and a `WARNING` is logged, but the
message itself is gone either way - MQTT's QoS here guarantees delivery to
the *client*, not to a *successfully-completed callback*, unlike
BlazingMQ's real at-least-once push-consume.

A real bug surfaced and fixed while verifying the failure path (not just
the happy path): building the `WARNING` message directly from
`edata->message` (the `CopyErrorData()`'d error text) *after*
`FlushErrorState()`/`AbortCurrentTransaction()` had already run corrupted
the heap (`pfree` on an invalid pointer, with the freed chunk's header
overwritten by fragments of that same message text) - reproduced
consistently, confirmed via a diagnostic build with a static message that
didn't crash. Fixed by copying `edata->message` into an independent
`std::string` immediately after `CopyErrorData()`, before anything else
runs - `CopyErrorData()`'s documented independence from `FlushErrorState()`
did not, in practice, extend far enough to survive
`AbortCurrentTransaction()` here. Verified fixed across repeated
consecutive callback failures, worker staying alive and processing
correctly throughout.

## Building

Requires Boost 1.88+ (system package `libboost-dev` on this machine already
ships `boost::mqtt5` - formerly the standalone Async.MQTT5 library, merged
into Boost - confirmed via `/usr/include/boost/mqtt5.hpp`; no vendoring
needed, unlike `pg_blazingmq`'s BDE/NTF/bmq dependency chain). Modern
Boost.System (1.69+) is header-only by default on this system - no
`libboost_system` to link against (confirmed: `-lboost_system` fails to
find the library at all, so the Makefile doesn't try).

```bash
make
sudo make install
psql -d postgres -c 'CREATE EXTENSION pg_mqtt'
```

Target broker for development/testing: [NanoMQ](https://nanomq.io/),
already installed on this machine at `/usr/local/bin/nanomq` /
`/usr/local/bin/nanomq_cli` (not running as a system service by default -
start it manually for testing, e.g. `nanomq start`).

## Plan

Mirroring `pg_blazingmq`'s own phased build:

1. **Build/link line** (done) - proof-of-linkage against Boost.MQTT5.
2. **Publish path** (done) - `mqtt_publish_binary`/`text`/`json`/
   `jsonb(topic, payload, qos DEFAULT 0, retain DEFAULT false)`.
3. **Subscribe/push-consume** (done) - `mqtt_subscribe(topic, fn_oid, qos
   DEFAULT 0)` / `mqtt_unsubscribe`, mirroring `pgnats`'s `nats_subscribe`
   and `pg_blazingmq`'s `bmq_subscribe` background-worker precedent. See
   the ack-semantics limitation noted above - genuinely different from
   BlazingMQ's push-consume, not just a naming difference.
4. **Tests** - `pg_regress` suite against a real NanoMQ broker.
5. **Docs**.

Deliberately out of scope: `pgnats`'s NATS KV (`nats_get/put_*`) and object
store (`nats_get/put_file`) functions have no MQTT equivalent - MQTT's
closest analog (a single retained message per topic) is too weak a fit to
imitate them meaningfully, so this extension doesn't attempt to. Request/
reply (`nats_request_*`) is a possible later phase - MQTT 5's response-topic
+ correlation-data properties can express it, but it's real additional
machinery beyond NATS's built-in request/reply, not a straight port.
