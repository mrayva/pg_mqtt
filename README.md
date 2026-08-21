# pg_mqtt

A PostgreSQL extension for publishing/consuming MQTT messages directly from
SQL, in the same spirit as this project's sibling extensions `pg_blazingmq`
(BlazingMQ pub/sub) and `pg_zerialize` (binary row (de)serialization). The
SQL interface is deliberately modeled on `pgnats`'s function shape
(`mqtt_publish_binary`/`text`/`json`/`jsonb`, `mqtt_subscribe`/
`mqtt_unsubscribe`) for consistency across this workspace's messaging
extensions - the implementation is unrelated (C++/PGXS/Boost.MQTT5, not
Rust/pgrx), only the SQL-facing shape is shared.

## Status: Phase 1 (build/link line)

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
2. **Publish path** - `mqtt_publish_binary`/`text`/`json`/`jsonb(topic,
   payload, qos DEFAULT 0, retain DEFAULT false)`.
3. **Subscribe/push-consume** - `mqtt_subscribe(topic, fn_oid, qos DEFAULT
   0)` / `mqtt_unsubscribe`, mirroring `pgnats`'s `nats_subscribe` and
   `pg_blazingmq`'s `bmq_subscribe` background-worker precedent.
4. **Tests** - `pg_regress` suite against a real NanoMQ broker.
5. **Docs**.

Deliberately out of scope: `pgnats`'s NATS KV (`nats_get/put_*`) and object
store (`nats_get/put_file`) functions have no MQTT equivalent - MQTT's
closest analog (a single retained message per topic) is too weak a fit to
imitate them meaningfully, so this extension doesn't attempt to. Request/
reply (`nats_request_*`) is a possible later phase - MQTT 5's response-topic
+ correlation-data properties can express it, but it's real additional
machinery beyond NATS's built-in request/reply, not a straight port.
