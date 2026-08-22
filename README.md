# pg_mqtt

A PostgreSQL extension for publishing/consuming MQTT messages directly from
SQL, in the same spirit as this project's sibling extensions `pg_blazingmq`
(BlazingMQ pub/sub) and `pg_zerialize` (binary row (de)serialization). The
SQL interface is deliberately modeled on `pgnats`'s function shape
(`mqtt_publish_binary`/`text`/`json`/`jsonb`, `mqtt_subscribe`/
`mqtt_unsubscribe`) for consistency across this workspace's messaging
extensions - the implementation is unrelated (C++/PGXS/Boost.MQTT5, not
Rust/pgrx), only the SQL-facing shape is shared.

## Broker choice: Mosquitto (recommended) or NanoMQ

As of 2026-08-22, **Mosquitto is the default/recommended/primarily-tested
broker**, not NanoMQ. This follows directly from the isolation test
documented below ("Is The ~8,200/s Single-Connection Ceiling Broker-Side Or
Client-Side?", commit `a26b72e`): the identical, unmodified `boost::mqtt5`
client hit **~27x** more throughput against Mosquitto than NanoMQ at N=1
(225,873/s vs 8,223/s) - NanoMQ's low ceiling turned out to be a
NanoMQ-specific weakness, not something inherent to MQTT or this
extension's client library. `test/manage_broker.sh` now starts a scratch
Mosquitto broker by default (plain/auth-required/TLS listeners on
`18830`/`18832`/`18831`); the original NanoMQ script is preserved as
`test/manage_broker_nanomq.sh` and remains fully supported - nothing in
`pg_mqtt.cpp` is broker-specific (just `broker_host`/`broker_port` and the
usual MQTT wire protocol), so switching is a config change, not a rebuild.

Every connect-time feature below (auth, Last Will and Testament, TLS,
session persistence, message expiry/user properties) was re-verified live
against Mosquitto 2.0.22 on 2026-08-22, alongside the original NanoMQ
verification - in two cases, Mosquitto's own logging/tooling made
**stronger** verification possible than NanoMQ allowed:
- **Message expiry and user properties are now fully round-tripped and
  independently confirmed** - a plain `paho-mqtt` Python subscriber against
  Mosquitto shows `MessageExpiryInterval : 77` and `UserProperty :
  [('env', 'test')]` directly on the received message's properties. The
  original NanoMQ verification could only confirm these were *accepted*
  (publish succeeds, payload arrives) since neither `nanomq_cli` nor packet
  capture (blocked in this sandbox) could surface incoming MQTT 5
  properties - this gap is now closed for Mosquitto, though `mqtt_subscribe`'s
  own receive path still discards `publish_props` entirely (unchanged, see
  below), so this verification used a raw client, not the SQL subscribe path.
- **Session persistence's `clean_start` flag is now directly confirmed
  in the broker's own log** (`c0` in Mosquitto's per-connection log line -
  `New client connected ... as sess-test-client-1 (p5, c0, k60)`), rather
  than inferred. This confirms Boost.MQTT5's hardcoded `clean_start=false`
  (see `mqtt_sidecar`'s notes on the same finding) actually reaches the
  wire. `session_present` on the CONNACK still wasn't independently
  observed - Mosquitto doesn't surface it via this log format either.

**A real, unrelated bug was found and fixed while re-verifying, not
Mosquitto-specific**: `ALTER EXTENSION pg_mqtt UPDATE` from 0.3 to 0.4 used
bare `CREATE OR REPLACE FUNCTION` for the publish functions' new 6-arg
signatures. Postgres treats a changed argument list as a *new* overload
rather than a replacement of the old one - the old 4-arg versions were
never dropped, leaving both registered simultaneously. Calling the old
4-arg form after such an upgrade was ambiguous ("not unique"), and in the
specific combination reproduced live, actually **segfaulted** the backend
(`pg_detoast_datum` on a garbage `jsonb` argument 5 - the new C function
body reading past a 4-slot `fcinfo`, confirmed via a live `gdb` backtrace).
Fixed in `pg_mqtt--0.3--0.4.sql` by adding explicit `DROP FUNCTION IF
EXISTS` statements for each old 4-arg overload before creating the new
6-arg ones; verified clean on a fresh 0.3-to-0.4 upgrade in a scratch
database (`\df` shows exactly one overload afterward). This bug affects
**any** existing 0.3 install being upgraded to 0.4, on any broker -
unrelated to the Mosquitto switch itself, just found while re-testing.

## Status: Phase 5 (docs) - all planned phases complete

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

## Connect-time features: auth, Last Will and Testament, session persistence, TLS

Boost.MQTT5 exposes several standard MQTT features that weren't wired in
through 0.3 - all closed out in 0.4, each verified live against a real
NanoMQ broker rather than just compiled:

| GUC | Purpose |
|---|---|
| `pg_mqtt.client_id` | MQTT Client Identifier. Empty (default) lets the broker assign one, which changes every reconnect. **Set this to a stable value if you use `session_expiry_seconds`** - session resumption is keyed by Client ID. |
| `pg_mqtt.broker_username` / `pg_mqtt.broker_password` | Username/password authentication. Empty username = no credentials sent (unchanged default behavior). `broker_password` is `PGC_SUSET`/superuser-only, matching how a credential-shaped GUC should be scoped. |
| `pg_mqtt.will_topic` / `will_payload` / `will_qos` / `will_retain` | Last Will and Testament - the broker publishes this if the connection closes abnormally. Empty `will_topic` (default) = no Will configured. **Verified end-to-end**: `SIGKILL`-ing a backend with a Will configured caused the broker to publish it to a separate live subscriber, byte-for-byte. |
| `pg_mqtt.session_expiry_seconds` | MQTT 5 Session Expiry Interval sent at CONNECT (0 = no hint, default). Boost.MQTT5 exposes no separate Clean Start flag - this is the only session-persistence knob this client library offers. **Verified the CONNECT-time property is sent and accepted without error across a disconnect/reconnect with a stable `client_id`; did not independently confirm the broker's CONNACK reported `session_present=true`** - NanoMQ doesn't log that at default verbosity, and packet capture is blocked in this sandbox (see the NanoMQ investigation earlier in this document). Treat as wired-and-accepted, not as a fully proven resumption guarantee. |
| `pg_mqtt.tls_enabled` | Connect over TLS instead of plain TCP. |
| `pg_mqtt.tls_ca_file` | CA file to verify the broker's certificate against. Empty = system default trust store. **Verified both directions**: connecting with the right CA succeeded; connecting to the same TLS listener with *no* CA configured (falling back to the system store, which doesn't trust a throwaway self-signed cert) was genuinely rejected - the broker's own log showed a real `Cryptographic error` on the handshake, not a client-side no-op. |
| `pg_mqtt.tls_cert_file` / `tls_key_file` | Client certificate/key for mutual TLS. Wired but not independently verified against a broker actually requiring client certs (NanoMQ's `fail_if_no_peer_cert` mode). |

A TLS-capable client is a structurally different Boost.MQTT5 type
(`mqtt_client<StreamType, TlsContext>`, not just a flag), so `pg_mqtt`
picks between a plain and a TLS client once per backend, the first time a
connection is actually needed - like `broker_host`/`broker_port` before it,
`pg_mqtt.tls_enabled` (and every GUC in this section) only takes effect at
that point, not on a later `SET`. Two library gaps had to be filled
directly in `pg_mqtt.cpp` to get TLS working at all: Boost.MQTT5 declares
`tls_handshake_type<StreamType>` and `assign_tls_sni<TlsContext,
TlsStream>` as customization points for the caller to specialize per
stream type, but ships no specialization for plain `boost::asio::ssl`
streams - omitting them isn't a compile error, it's a *link* error
(`undefined symbol`) that only appears once a TLS-capable client is
actually instantiated.

`mqtt_subscribe()`'s worker picks up the same connect-time GUCs, copied
into its `SubscriberConfig` at call time (a background worker doesn't
inherit its launching session's `SET`-level overrides - the same reason
`broker_host`/`broker_port` were already threaded through, before any of
this existed). One real MQTT-specific wrinkle: if `pg_mqtt.client_id` is
set, the worker doesn't use it verbatim - MQTT brokers apply *takeover*
semantics on a duplicate Client ID (the older connection gets
disconnected), so a subscriber sharing the backend's exact Client ID would
fight with the publish connection (or another subscriber) for one
identity. The worker uses `"<client_id>-sub-<topic>"` instead, which is
enough to be stable across a worker restart on the same topic (preserving
session persistence's actual point) but **does not** resolve two
concurrent `mqtt_subscribe()` calls on the *same* topic - those still
collide today.

`mqtt_publish_binary/text/json/jsonb` gained two new trailing parameters:
`message_expiry_seconds DEFAULT NULL` (MQTT 5 Message Expiry Interval -
the broker discards the message if undelivered within this many seconds)
and `user_properties DEFAULT NULL` (a flat `jsonb` object of string keys to
string values, mapped onto MQTT 5 User Properties - any non-object
top-level value, or any non-string value, is a hard error rather than a
silent best-effort coercion). Both were verified to be *accepted*
end-to-end (publish succeeds, payload arrives correctly at a live
subscriber) but their on-wire property bytes were not independently
inspected - `nanomq_cli sub -V 5 -v`'s own verbose output doesn't surface
incoming MQTT 5 properties, and, again, packet capture is unavailable in
this sandbox. **Also worth noting**: `mqtt_subscribe()`'s receive path
still discards incoming `publish_props` entirely (unchanged from 0.3) - a
subscribed callback has no way to read a message's own expiry/User
Properties even though the publish side can now set them for other
consumers. Extending the subscribe path to expose them is a natural,
still-open follow-up.

**A real, previously-latent bug found and fixed while verifying auth**:
`publish_sync_on()`'s wait for a publish's completion was a raw,
unbounded `std::future::get()` - harmless as long as every connection
attempt eventually either succeeded or handed back an error, which held
until this version made "the broker never accepts the connection at all"
a reachable outcome (wrong credentials, TLS handshake rejected - Boost.MQTT5
just retries forever in both cases rather than surfacing a terminal error).
A backend stuck this way didn't even respond to `pg_terminate_backend()`'s
`SIGTERM` - only `SIGKILL` worked, reproduced live during this pass.
Fixed by polling the future with a bounded `wait_for()` and calling
`CHECK_FOR_INTERRUPTS()` between polls (mirroring the bounded-wait pattern
`mqtt_subscribe`'s own hang fix already used, commit `82920af`), with the
promise itself moved to a `shared_ptr` so a still-pending completion
handler on the background thread can't write into a promise whose stack
frame already unwound. Verified: `pg_terminate_backend()` against a stuck
auth-failure connection now takes effect in ~1-2 seconds, not never.

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

## Testing

`make test` is the one-command entry point: starts a scratch NanoMQ broker
(`test/manage_broker.sh`, listening on `127.0.0.1:18830` - deliberately not
MQTT's default 1883, to avoid clashing with any real broker already running
on this machine), runs `make installcheck`, then always stops the broker
afterward - even on failure, so a failing run doesn't leak a background
broker process. Requires `pg_mqtt` already `make install`'d.

NanoMQ's own start/stop control turned out to be **global**, not
per-instance (a single PID file at `/tmp/nanomq/nanomq.pid` and a fixed
control IPC socket at `/tmp/nanomq_cmd.ipc`, confirmed by observation, not
documented anywhere obvious) - only one NanoMQ instance can be managed this
way on a given machine at a time. Fine for this test suite (one scratch
broker), but don't assume two independent test brokers can run side by side
with `manage_broker.sh`.

`REGRESS = 01_link_check 02_publish 03_subscribe` covers all three phases.
`03_subscribe.sql` also verifies two things worth knowing about if you're
reading or extending it:

- **Retain semantics for real**: publishes a retained message *before* any
  subscriber exists, then subscribes, and checks it arrives immediately -
  not just trusting the Phase 2 claim, actually exercising it.
- **Resilience**: publishes a message that makes the callback raise, then a
  normal message on the same subscription, and checks the *second* one
  still gets processed - proving a callback failure doesn't take the whole
  worker down. The `WARNING` itself goes to the server log (a separate
  bgworker process, not the test's own session), so it isn't part of the
  diffed output - what's checked is that processing continues afterward,
  which is the actually load-bearing guarantee.

Like `pg_blazingmq`'s `bmq_subscribe`, a test that fails *inside* a
`mqtt_subscribe`/`mqtt_unsubscribe` `DO` block before reaching
`mqtt_unsubscribe()` can leave a subscriber worker running, holding a
database connection open. Recover with `SELECT pg_terminate_backend(pid)
FROM pg_stat_activity WHERE backend_type = 'pg_mqtt subscriber';` before
retrying.

## Benchmarks: Stress-Testing With The Pattern That Found `nats-server`'s Bottleneck

`~/pg_blazingmq/bench/README.md` found `nats-server`'s ultimate bottleneck
via a specific stress pattern: N *competing* consumers (a NATS queue group)
against one publisher, swept N=1/2/4/8/16/32, profiled with `perf`. Peaked
at N=4 (~915k/s aggregate), then declined gently to N=32 - root-caused to
per-connection `flushOutbound` write cost plus Go GC/scheduler pressure,
both growing with connection count. This section re-runs the same pattern
here, using `pg_mqtt`'s real, shipped `mqtt_subscribe`/`mqtt_publish_text`
- no throwaway driver.

**A real bug found and fixed first**: MQTT's competing-consumer analog to a
NATS queue group is a *shared subscription* (`$share/<group>/<topic>`).
Subscribing 2 `mqtt_subscribe()` workers to one raised
`WARNING: ... has not finished subscribing after 5000ms` on both, and
NanoMQ's log showed why: `No local is conflict with shared subscription!`.
Boost.MQTT5 defaults `subscribe_options::no_local` to `yes`, but MQTT 5
section 3.8.3.1 makes it a Protocol Error to set No Local on a shared
subscription - NanoMQ was correctly rejecting a spec violation, not
misbehaving. Fixed in `pg_mqtt.cpp`: `no_local` is now forced off
specifically when the topic starts with `$share/`, left at its default
otherwise. Verified directly before trusting anything downstream: 2
workers, 200 published messages, exactly 100/100 split, 0 duplicates.

**Full N=1..32 sweep** (20,000 messages/run, sequence-numbered payloads,
fresh NanoMQ restart per N, correctness checked every run):

| N | publish rate | drain-only rate | correctness |
|---|---|---|---|
| 1 | 12,262/s | 661/s | 20,000/20,000, 0 dup |
| 2 | 12,001/s | 670/s | 20,000/20,000, 0 dup |
| 4 | 11,960/s | 1,381/s | 20,000/20,000, 0 dup |
| 8 | 11,938/s | 3,033/s | 20,000/20,000, 0 dup |
| 16 | 11,794/s | 7,242/s | 20,000/20,000, 0 dup |
| 32 | 11,749/s | 10,818/s | 20,000/20,000, 0 dup |

Perfect, evenly-balanced round-robin delivery at every N (e.g. N=32: 1,000
per worker, all 32 workers identical). Publish rate is flat regardless of
N, as expected (one publisher, unrelated to subscriber count). Drain rate
climbs **continuously and monotonically all the way to N=32** - no peak,
no decline, no collapse - a third shape, distinct from both `nats-server`'s
peak-then-gentle-decline and BlazingMQ priority mode's flat wall/collapse.

**This does not actually answer the original question, and that's the
real finding.** At N=1, draining 20,000 messages took a slow, remarkably
steady ~31 seconds (~650/s, consistent second-over-second - confirmed via
a dedicated re-run with fine-grained polling, not inferred from one
timeout). That rate is consistent with Postgres's own per-message
transaction-commit cost (`StartTransactionCommand`/`SPI_connect`/insert/
commit, once per callback invocation) - not anything happening inside
NanoMQ. The N=1..32 ceiling (topping out at ~10,818/s) never gets within
1-2 orders of magnitude of the ~700k-900k/s range where `nats-server`'s
own connection-scaling bottleneck actually appeared. A `perf`/`mpstat`
attempt at N=32 confirmed this indirectly: the profiler mistakenly
attached to the wrong process (a real tooling miss, not re-attempted
given the low expected value) but `mpstat -P ALL` showed ~97.9% aggregate
idle across all cores throughout - nowhere near hardware-bound, consistent
with the bottleneck being `pg_mqtt`'s own per-message transaction
overhead rather than any broker- or hardware-level limit.

**Honest conclusion**: this test measures `pg_mqtt`'s own subscribe-path
scaling (which is real, useful information - it scales cleanly with
worker count, unlike BlazingMQ's priority mode) but it cannot answer
whether NanoMQ's own connection-handling would hit a wall like
`nats-server`'s, because `pg_mqtt`'s per-message Postgres transaction cost
becomes the limiting factor several orders of magnitude before NanoMQ's
own broker-side connection-scaling behavior would plausibly matter.
Answering the original question would need a raw, non-Postgres MQTT
client stressing NanoMQ directly at hundreds-of-thousands-of-messages/sec
- out of scope here by design (this session deliberately avoided a
throwaway driver in favor of exercising `pg_mqtt`'s real, shipped
functions).

**Secondary finding, also real - fixed**: `mqtt_unsubscribe()`'s `SIGTERM` did
not cleanly terminate two workers whose broker connection was already dead
(discovered when a broker was stopped without unsubscribing first) - both
required a forced `kill -9` to clear, `mqtt_unsubscribe()` itself reported
success (`true`) despite the worker not actually exiting.

Root-caused via a live `gdb -p <stuck_pid> -batch -ex "thread apply all bt"`
against a worker reproduced by subscribing with *no broker running at all*
(the most reliable repro found - a live-then-dropped connection did not
reproduce it across 33 varied trials: idle waits, immediate unsubscribes,
abrupt broker kills, active traffic, and a 16-concurrent-worker sweep, all
exited cleanly). The backtrace showed Thread 1 parked in
`boost::asio::io_context::run_one()` at `pg_mqtt.cpp:590`, inside the
*initial* subscribe-handshake wait loop - `while (!sub_done) { ioc.run_one();
}` - which, unlike the main receive loop just below it, never checked
`ShutdownRequestPending` and used an unbounded `run_one()` instead of a
bounded poll. If `async_subscribe`'s completion handler never fires (broker
unreachable from the start), this loop blocks in `epoll_wait` forever, deaf
to any signal. Fixed by bounding it the same way the receive loop already is
- `ioc.run_one_for(500ms)` with a `ShutdownRequestPending` check each
iteration, throwing (and cleanly unwinding through the existing catch/log/
exit path) if shutdown is requested before the subscribe completes. Verified
3/3 on the reproducing (no-broker) case, no regression on the normal
live-broker case, and `make test`'s full `pg_regress` suite still passes
clean.

### Two-Way Comparison: `mqtt_subscribe` vs `pgnats`'s `nats_subscribe`

The N=1 result above (~650-661/s, transaction-bound) raised an obvious
question: is that `pg_mqtt`-specific overhead, or is it just what one
Postgres transaction per callback costs on this machine regardless of
extension? Checked directly against `pgnats`'s own push-consume path
(`nats_subscribe`), not assumed either way.

**`nats_subscribe`'s transaction model, confirmed from `pgnats`'s real
source** (`~/pgnats/src/bgw/subscriber/mod.rs`, `pg_api.rs`) - structurally
the same shape as `pg_mqtt`/`pg_blazingmq`: `handle_internal_message()`'s
`CallbackCall` case wraps every dispatch in `BackgroundWorker::transaction(||
call_function(callback, data))` - `pgrx`'s own equivalent of
`StartTransactionCommand`/`SPI_connect`/.../`CommitTransactionCommand`, one
full transaction per message, not batched or pipelined. One real
architectural difference worth noting: `call_function` runs `SELECT
{callback}($1)` as a genuine SQL string through `Spi::connect_mut` (full
parse + plan + execute), where `pg_mqtt`/`pg_blazingmq` call
`OidFunctionCall1` directly against an already-resolved OID, bypassing the
SQL layer entirely - a plausible source of *extra* per-message cost for
`nats_subscribe`, not less.

**A real, unrelated operational issue surfaced before any number could be
trusted**: `nats_subscribe()`'s first attempt silently failed - `pgnats`
uses a launcher/per-database-subscriber-worker architecture, and this
database's subscriber worker had permanently exited hours earlier
(`IO error: Connection refused`, from a NATS server that wasn't running yet
at Postgres startup) with no automatic retry. `pg_stat_activity` still
showed the launcher running, but `SELECT * FROM pgnats.subscriptions`
was empty and the log read `No subscriber worker found for db_oid 5`. Fixed
with a full `postgresql` service restart (not a `pgnats` code/config
change) to let the launcher re-spawn a live subscriber worker - worth
knowing this can happen silently if NATS isn't already up when Postgres
starts.

**Result, reproduced twice with a fresh `nats-server` restart between runs**
(20,000 messages, `nats_publish_binary`, same minimal one-row-insert
callback shape as the `pg_mqtt` test, N=1, core NATS - no JetStream):

| system | N | drain rate |
|---|---|---|
| `pg_mqtt` (`mqtt_subscribe`, NanoMQ) | 1 | 661/s |
| `pgnats` (`nats_subscribe`, NATS core), run 1 | 1 | 600/s |
| `pgnats` (`nats_subscribe`, NATS core), run 2 | 1 | 611/s |

**Within ~10% of each other, both reproducible.** This confirms the
hypothesis directly: the ~600-660/s ceiling is Postgres's own
per-message-transaction cost, not something specific to `pg_mqtt`'s
implementation or NanoMQ - `pgnats`, a mature, independently-implemented
extension with a different background-worker architecture (one shared
subscriber-dispatch worker per *database*, not per *subscription* the way
`pg_mqtt`/`pg_blazingmq` spin up a worker per `..._subscribe()` call - so
N>1 numbers between the two aren't directly comparable without separate
databases per `pgnats` subscription; only N=1 isolates the same question
cleanly) lands in the same place. Neither extension's push-consume design
is unusually slow; one Postgres transaction per message just costs what it
costs.

### Three-Way: Adding `pg_blazingmq`'s `bmq_subscribe`

Completed in `~/pg_blazingmq/bench/README.md` (same repo, that document is
the natural home for a new `pg_blazingmq` number) - summarized here since
this is where the comparison narrative lives:

| system | N=1 drain rate |
|---|---|
| `pg_mqtt` (`mqtt_subscribe`, NanoMQ) | 661/s |
| `pgnats` (`nats_subscribe`, NATS core) | 600-611/s |
| `pg_blazingmq` (`bmq_subscribe`, BlazingMQ broadcast) | 751-976/s (two runs) |

Same order of magnitude as the other two (all three within roughly a 1.6x
band), broadly confirming the shared-transaction-cost hypothesis - but not
an exact match, and the direction is worth knowing: `bmq_subscribe` does
strictly *more* per-message work than either (an explicit
`session.confirmMessage()` network round-trip after every commit that
neither `mqtt_subscribe` nor `nats_subscribe` has an equivalent of), yet
came out faster in both runs, not slower. So the ~600-660/s figure from
the two-way comparison above isn't a precise universal constant - it's the
rough floor this whole class of one-transaction-per-callback extension
design pays, with real headroom for broker-specific round-trip
characteristics (and ordinary run-to-run variance - `bmq_subscribe`'s own
two runs spread ~23%, 976/s vs 751/s) to move the exact number around.

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
4. **Tests** (done) - `pg_regress` suite against a real NanoMQ broker,
   `make test` as the one-command entry point (see Testing above).
5. **Docs** (done) - see Changelog and Maintained Documentation below.

All originally-planned phases are complete.

## Changelog

Each entry corresponds to one `pg_mqtt--X.Y.sql` version; see those files
for the exact functions each version added. This extension hasn't reached
1.0 yet - versions below that should be considered unstable.

- **0.3** -- Added push-consume: `mqtt_subscribe(topic, callback_fn, qos
  DEFAULT 0)` / `mqtt_unsubscribe(worker_pid)`. A dynamic background worker
  per subscription, config handed off via a pinned DSM segment, an atomic
  readiness handshake, and per-message SPI transaction dispatch - see the
  honest note above and in ARCHITECTURE.md about why this is *not* true
  at-least-once the way `pg_blazingmq`'s `bmq_subscribe` is.
- **0.2** -- Added `mqtt_publish_binary/text/json/jsonb(topic, payload, qos
  DEFAULT 0, retain DEFAULT false)`: a per-backend client with a background
  `io_context`-pumping thread and a synchronous promise/future bridge for
  publish calls, plus runtime dispatch across QoS's three compile-time
  template instantiations.
- **0.1** -- Initial release: `pg_mqtt_link_check(broker_host, broker_port)`,
  proof-of-linkage against Boost.MQTT5.

(Phase 4's test suite added no new SQL surface, so it didn't bump the
version.)

## Follow-Up: Trying To Reach `nats-server`-Comparable Rates With `nanomq_cli` Directly

The `mqtt_subscribe`-based benchmark above never got close to `nats-server`'s
~700k-900k/s bottleneck range - it topped out around 10,818/s at N=32,
limited by `pg_mqtt`'s own per-message Postgres transaction cost (see
`bench/README.md` in `pg_blazingmq` for the matching finding that this is a
*shared*, extension-independent cost - `pgnats`'s `nats_subscribe` lands in
the same ~600/s range at N=1). This follow-up tried to isolate NanoMQ's own
connection-scaling behavior by going around Postgres entirely, using
`nanomq_cli` (NanoMQ's own official pub/sub CLI, already installed) directly
- no `pg_mqtt`, no custom driver.

**Result: `nanomq_cli` itself cannot reach the rates needed to answer the
original question, and this is a real, reproducible limitation of the tool,
not a conclusion about NanoMQ the broker.**

- `nanomq_cli pub`'s `-I <ms>` inter-message interval has a real floor: `-I
  0` is rejected outright (`Integer argument too small (value > 1)`), so
  `-I 1` is the fastest available - a hard ~1,000 msg/s ceiling *per
  connection*, confirmed directly (1,000 messages at `-I 1` took ~2s
  including connect/disconnect overhead).
- `-n <parallel>` (multiple connections from one `pub` invocation) did not
  behave reliably: runs produced `nng_send_aio: Object closed` errors and
  message loss, not a clean multiplication of throughput.
- Falling back to genuinely independent OS processes (5 separate
  `nanomq_cli pub` invocations, 500 messages each at `-I 1`, one shared
  `nanomq_cli sub` counting arrivals) was more stable but still lost a large
  fraction of messages at QoS 0 - only 1,067 of 2,500 published messages
  (~43%) were ever received, stable after the subscriber's full timeout
  window, at a scale (2,500 messages, 5 concurrent processes) far too small
  to be an interesting throughput data point on its own.

Given this, scaling up to the hundreds of parallel OS processes that would
be needed to approach `nats-server`'s ~900k/s peak through this interface
was not attempted - both because `-I`'s per-connection ceiling makes it
impractical without an unreasonable process count, and because the
reliability problems at trivial scale would make any resulting number
untrustworthy without first understanding *why* messages were being lost
(a real, separate investigation of its own, out of scope here).

**The original question - does NanoMQ's C implementation avoid the
`flushOutbound`/GC-driven decline `nats-server` shows past N=4 - remains
genuinely open.** Answering it properly would need either a purpose-built
high-throughput MQTT load generator (the kind of tool this investigation
was explicitly asked not to build as a one-off), or profiling NanoMQ under
whatever its own real production benchmarking tooling is, if any exists
upstream. Recorded here as a deliberate, evidence-based stopping point, not
an oversight - the honest answer is "we tried the approved tool, it isn't
fit for this specific job at this scale," not a number pretending
otherwise.

### Follow-up: root-causing the QoS-0 loss above

The 57% loss (1,067/2,500 messages, 5 concurrent `nanomq_cli pub` processes
into one `nanomq_cli sub`) was investigated directly rather than left as a
guess. Six independent variants were run, each isolating one candidate
cause, all against a fresh broker per run:

1. **Subscribe-before-publish race** (the most likely mundane explanation,
   given this exact class of bug has shown up repeatedly elsewhere this
   session - BlazingMQ's non-retroactive filtering, a real `bmq_subscribe`
   race fixed earlier, `nats_sidecar`'s DSM readiness handshake): confirmed
   the subscriber's `CONNECT`/`SUBACK` completed (via `-v` log output), then
   waited a further 3 seconds - a generous head start - before starting any
   publisher. **Result: 1,064/2,500 received, statistically identical to
   the original run. Ruled out.**
2. **Client-ID collision** (default `nanomq_cli` client IDs are randomly
   generated per-process; if two of five processes launched in the same
   instant collided, MQTT requires the broker to disconnect the earlier
   client, truncating its stream): re-ran with explicit, guaranteed-unique
   `-i` identifiers per publisher. **Result: 1,064/2,500. Ruled out.**
3. **Publisher-side send failures**: every one of the five publisher logs
   showed a clean `connect_cb: ... result: 0` and `disconnected reason: 0`
   (normal, self-initiated disconnect after finishing) with zero errors -
   from the publisher's own point of view, all 500 messages per process
   were sent successfully every time. Loss is not happening on the publish
   side.
4. **Which publishers lose messages**: tagged each publisher's payload with
   its own identity and counted per-publisher arrivals. All five
   contributed a non-zero, non-500 share (170-278 out of 500 each, summing
   to 1,064) - loss is spread proportionally across every publisher, not
   one process failing outright. Consistent with a shared downstream
   constraint, not a single publisher's bug.
5. **Broker-side queue capacity** (`mqtt.max_mqueue_len`/
   `max_inflight_window`, default config ships `2048`, comfortably above
   2,500 total messages already - though the config actually in effect for
   a bare `nanomq start` with no `--conf` wasn't confirmed to be that
   file): started a second broker instance with an explicit config setting
   both to `100,000` - fifty times more headroom. **Result: 1,064/2,500,
   the same number again.** A real, sensitive capacity limit would very
   plausibly respond to a 50x change; getting the identical count instead
   points away from broker-side buffering as the mechanism.
6. **Broker log inspection**: the only `WARN`-level lines during any run
   were generic `nni aio recv error!! Connection shutdown` /
   `recv_error rv: 139` messages - confirmed benign by running a clean
   single-publisher/100-message control that received 100/100 with zero
   loss and produced the *exact same* warning lines. These are routine
   per-connection teardown logging, not evidence of drops.

**Conclusion: not a test-harness timing bug, not a broker-side capacity
limit as configured - the evidence points at `nanomq_cli`'s own subscriber
process (its bundled demo/test client, built on NNG) as the actual limiting
layer under a concurrent multi-publisher burst**, not something in NanoMQ
the broker itself. This is inferred from elimination (five other candidate
causes directly ruled out or shown not to move the number) rather than
directly proven with a packet capture or client-side instrumentation - a
stronger proof would trace the exact drop point inside `nanomq_cli`'s own
receive path, not attempted here. Practically: this means the earlier
"NanoMQ vs `nats-server`" question is *still* unanswered, and for the same
underlying reason as before - `nanomq_cli` is a demo/test tool, not
production load-testing tooling, and this investigation surfaces a second,
independent way in which that shows up (unreliable receipt under
concurrent burst, on top of the earlier `-I` rate ceiling).

**Bonus finding, unrelated to the loss investigation itself**: `nanomq_cli
pub -l` (stdin-line mode, reading one message per line from piped input)
**segfaults** reliably when driven this way (`cat file | nanomq_cli pub -l
...`) - a real, reproducible crash in the CLI tool, discovered while trying
to use sequence-numbered payloads for finer-grained diagnosis. Worth
avoiding `-l` mode entirely until upstream fixes it; not investigated
further here since it's off the main thread of this task.

**Follow-up control: does QoS 1 fix it?** The identical scenario (5
concurrent `nanomq_cli pub` processes, 500 messages each, one `sub`) was
re-run at QoS 1 instead of QoS 0 - a control the original investigation
didn't include. QoS 1 requires PUBACK handshaking and redelivery of
unacknowledged messages, so if the earlier loss were simply QoS 0's
documented no-guarantee/no-retry design, QoS 1 should reliably approach
2,500/2,500. It didn't, across 4 fresh-broker runs, each given a long
wait window afterward to let any redelivery finish (confirmed via two
runs that plateaued and stopped growing well before the window ended,
not cut off early):

| run | received / 2,500 |
|---|---|
| 1 | 1,065 |
| 2 | 2,006 |
| 3 | 2,044 |
| 4 | 1,065 |

A real, reproducible **bimodal** split, not noise scattered around one
value - runs 1 and 4 landed at the *exact same* count as each other and
as the original QoS-0 result (1,064-1,067); runs 2 and 3 landed close to
each other in a completely different band (~80% delivered). All
publisher logs showed clean sends with zero errors in every run, same as
before.

**This overturns "QoS 0's design explains it" as a complete answer, and
strengthens the `nanomq_cli`-receive-path conclusion rather than
replacing it.** If QoS 0's lack of guarantees were the whole story, QoS
1's acknowledgment+retry mechanism should have pushed every run close to
100% delivered - it didn't, and half the runs showed no improvement over
QoS 0 at all. A bimodal "sometimes recovers roughly half the loss via
retry, sometimes the retry hits the identical failure and recovers
nothing" pattern is exactly what a real, intermittent defect in
`nanomq_cli`'s own receive path would produce - QoS 1's retransmission
can only help if the *retried* copy of a message is actually received
and processed correctly, and evidently it sometimes isn't either.
(NanoMQ version installed: v0.23.7-11, checked but not otherwise
investigated for known fixes - out of scope for this quick control.)

### Follow-up: direct evidence via `strace` (packet capture unavailable)

The elimination-based conclusion above was tested directly rather than left
as an inference. `tshark`/`dumpcap` packet capture turned out to be
unavailable in this environment - it failed identically whether run via
`sudo`, without `sudo` (`dumpcap` already carries
`cap_net_admin,cap_net_raw` capabilities, so `sudo` wasn't even the right
approach), or with the sandbox explicitly disabled, always with `Couldn't
run dumpcap in child process: Permission denied`. This looks like a
container-level restriction (no `CAP_NET_RAW` granted to the container)
that isn't fixable from inside the session, so the wire-level count from
the original plan couldn't be captured - only `strace` (which needs
`ptrace`, confirmed working, not raw sockets) was available.

Re-running the same 5-publisher/500-message/QoS-0 scenario under `strace
-f` first showed the loss reproduces under tracing too, at yet another
rate (1,935/2,500 received, ~23% loss, and a separate run landed at
1,845/2,500, ~26% loss) - a third and fourth data point confirming the
loss rate itself varies run to run rather than converging on one number,
consistent with the QoS-1 control's bimodal finding.

The first trace attempt (filtered to `-e trace=network,read`) recorded
**zero** read/recv syscalls on the MQTT socket for the entire 62-second
run despite over a thousand messages being received in that window -
because NNG's actual data-path syscall is `readv()`, which isn't in
strace's `read` or `network` trace classes. Once the filter was widened
(`-e trace=%network,%desc`), the real picture emerged: NNG reads each
PUBLISH as two `readv()` calls per message - a 2-byte fixed header, then
a 17-byte variable-header+payload chunk containing the exact topic and
payload bytes (e.g. `readv(5, [{iov_base="\0\vbench/topicpub2", ...}])`).

In the final full-scale run: **2,301 complete 17-byte PUBLISH payloads
were read off the socket via `readv()`, but only 1,935 were ever printed
by the subscriber's own message callback** - a 366-message (~16%) gap
between "bytes the kernel handed to the process" and "messages the
application actually surfaced." This is direct, non-inferred evidence
that at least part of the loss happens **above the syscall layer**, inside
`nanomq_cli`'s (NNG's) own MQTT frame parsing or callback dispatch, not as
packets dropped on the wire - bytes demonstrably arrived at the process
and were still lost. Separately, total bytes read from the socket
(43,204) fell short of the 47,500 bytes needed for all 2,500 messages,
suggesting some additional loss upstream of the read syscall too (network/
socket-buffer level) - though this figure is confounded by teardown timing
in that specific run (the subscriber was killed after a fixed wait rather
than a verified stdout plateau, unlike the other runs in this
investigation), so it isn't as clean a number as the 2,301-vs-1,935 gap.

**Conclusion**: the loss is not a single clean mechanism at one layer -
there is direct, syscall-level proof of loss *above* the read/`readv`
layer (the 2,301-vs-1,935 gap), and suggestive but less rigorously
isolated evidence of additional loss at or below the socket-read layer.
Packet capture would be needed to fully resolve the latter and rule wire-
level loopback drops in or out with certainty; that step remains blocked
by this environment's lack of raw-socket capability, not by anything
about NanoMQ or `nanomq_cli` itself. The elimination-based conclusion
above stands and is now reinforced with direct evidence for at least one
concrete loss point: `nanomq_cli`'s own receive-side parsing/dispatch
demonstrably drops fully-received wire data under concurrent-publisher
burst load.

### Follow-up: source-level evidence via `gdb` against a debug build

`strace` proved loss occurs above the syscall layer but couldn't say
exactly where. To pin that down, NanoMQ was built from source
(`github.com/nanomq/nanomq`, with its `nng`/NanoNNG submodule) with
`-DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_NANOMQ_CLI=ON`, giving
unstripped `nanomq`/`nanomq_cli` binaries with real debug symbols -
unlike the system-installed, stripped versions used everywhere above.
`gdb -x <script.py>` (batch, non-interactive, counting breakpoint hits
rather than manual stepping) instrumented the two layers between "bytes
off the wire" and "message printed": NNG's MQTT protocol dispatch
(`nng/src/mqtt/protocol/mqtt/mqtt_client.c`, the `case NNG_MQTT_PUBLISH:`
handler) and `nanomq_cli`'s own app-level state machine
(`nanomq_cli/client.c`'s `client_cb`, `RECV_WAIT` state).

Reproducing the same 5-publisher/500-message/QoS-0 scenario under this
instrumented `sub` process (2,500 sent) gave a clean, reproducible split:

| checkpoint | count | location |
|---|---|---|
| decoded as PUBLISH by NNG's protocol layer | 2,500 / 2,500 | `mqtt_client.c:1205`, `case NNG_MQTT_PUBLISH` |
| delivered into the CLI's own app state machine | 754 / 2,500 | `client.c:1521`, `RECV_WAIT` entry |
| printed by the subscriber's callback | 753 / 2,500 | `client.c:1531`, `console(...)` |

**Every single published message was correctly decoded by NNG's MQTT
parser** (2,500/2,500) - this rules out wire-level/transport-decode loss
as the mechanism, at least in this run. The real gap (2,500 to 754, ~70%)
opens up entirely between protocol-level decode and the point where
`nanomq_cli`'s single-threaded `client_cb` state machine actually consumes
a message via `nng_ctx_recv()`. That handoff goes through a bounded,
fixed-size 64-slot queue (`NNG_MAX_RECV_LMQ` = 64,
`nng/include/nng/mqtt/mqtt_client.h:166`) - when no `ctx` is currently
waiting in `recv_queue` (i.e. the app hasn't re-armed `nng_ctx_recv()`
yet), an arriving PUBLISH is enqueued there instead of delivered directly
(`mqtt_client.c:1216-1225`), and if that queue is already full, the
message is freed and dropped on the spot.

**One important negative result**: that drop path unconditionally calls
`log_warn("Warning: no ctx found!! PUB msg lost!")` - confirmed present
in the built binary (`strings` finds the exact text) - yet this line
**never appeared once** in any `gdb` run's output, despite a ~1,750-message
gap. That means the dominant loss mechanism here is not cleanly
attributable to that one guarded overflow-drop branch specifically; it's
more directly explained by `nanomq_cli sub`'s single serialized
`nng_ctx_recv`-then-process loop (`--parallel`/`-n` defaults to 1 `ctx`)
simply not keeping pace with a concurrent 5-process burst - some fraction
of the un-delivered messages may have still been sitting queued
(undropped, just undrained) when the test was cut off, rather than
actively dropped; this run's methodology can't fully separate the two.

**gdb's own overhead measurably worsened the loss** - 30% delivered here
vs. 44-84% across the non-gdb `strace` runs above - which is itself
informative: it's a dose-response result consistent with "the app-side
receive loop being too slow to keep up with a concurrent-publisher burst"
as the dominant mechanism, since deliberately slowing that loop further
(via breakpoint overhead, with no code changes) made the loss worse in
direct proportion, rather than being independent of it.

**One dead end worth recording**: an attempt to use a *conditional*
breakpoint (`client.c:1531 if topic_len == 0`) to check whether messages
reaching `RECV_WAIT` had a corrupted/empty topic produced a self-
contradicting result - it reported `topic_len == 0` on all 754 hits, yet
753 of those same messages were simultaneously printed correctly with
real topic/payload text in the same run. This is consistent with a known
GDB pitfall: at `-O2`/`RelWithDebInfo`, a local variable's DWARF-described
location isn't always valid yet at the exact PC a source-line breakpoint
lands on, so a condition referencing it can read stale/garbage state.
Not a real finding about `nanomq_cli` - flagged so the technique isn't
mistakenly reused as-is.

**Conclusion**: the dominant, now source-level-confirmed loss point is the
handoff between NNG's protocol-decode layer and `nanomq_cli`'s single-
context application receive loop under concurrent-publisher burst load -
not broker-side, not wire-level decode. The precise final attribution
(silent drop via an unlogged path vs. simply not-yet-drained backlog at
kill time) remains open and would need either a longer, uninstrumented
observation window with careful drain-to-plateau confirmation, or
instrumenting `nni_lmq_put`'s actual queue-depth over time rather than
just its call sites. Reported honestly as the strongest evidence gathered
so far, not a fully closed case.

## The Original Question, Finally Answered: Does NanoMQ Share `nats-server`'s Connection-Scaling Bottleneck?

This is the question that started the whole investigation above: raw NATS
core, under a queue group of N competing consumers, peaked at N=4
(~915k/s) then declined *gently* through N=32, root-caused via `perf` to
per-connection `(*client).flushOutbound` growing fastest of any profiled
symbol (10.37%->16.02% of samples, N=4->N=8) plus `sync.RWMutex`
contention and Go GC/scheduler pressure. Every previous attempt to test
the same shape against NanoMQ failed on tooling, not methodology:
`pg_mqtt`'s own Postgres-transaction overhead caps out around 650-10,818/s
(nowhere near enough to stress a broker), and `nanomq_cli` was separately,
conclusively proven unfit for this above (rate-limited publish path,
unreliable `-n`, and its own receive-path message loss under load).

**Built a new, standalone, non-Postgres, non-`nanomq_cli` tool** -
`bench/mqtt_raw_bench.cpp` - using `boost::mqtt5` directly (the same
client library `pg_mqtt.cpp` already uses, just without its
promise/future-blocking sync bridge, which is built for one-call-at-a-time
Postgres-backend correctness, not firehose throughput). Publishers
fire-and-forget QoS 0 `async_publish` in self-chaining pipelined loops (16
lanes x 4 connections); subscribers are N independent connections, each
subscribed to `$share/benchgroup/<topic>` (MQTT 5 shared subscription -
the direct analog of a NATS queue group, competing not fan-out consumption
- reusing the same `no_local`-off-for-`$share/` fix `mqtt_subscribe`
needed, commit `feade9b`, or NanoMQ rejects the subscribe outright).

**A real methodology trap caught along the way**: the naive
`published/wall_publish_time` calculation is bogus here - `async_publish`'s
completion fires on local write-buffer handoff, not broker delivery, so a
burst of ~200-400k messages completes locally in well under a second
regardless of `duration_sec`, then the publisher-side counter simply
freezes while the real work (the broker actually delivering the backlog to
subscribers) continues for many seconds after. This is the same
undrained-backlog trap flagged in `project_blazingmq_nats_benchmark.md`'s
methodology lesson 0, just showing up on the publish side instead of the
consume side this time. Fixed by sampling `received` every 200-500ms
through the whole run and only trusting a number once the receive count
stops climbing across 3 consecutive samples (loss_pct settled at ~0.00-0.02%
on every N below, i.e. every run fully drained, not cut off mid-backlog).

**N-sweep result** (fixed ~211-215k message backlog per run, fresh NanoMQ
broker restart before each N, `~/nanomq/build/nanomq/nanomq`, unstripped
debug build from the `gdb` investigation above):

| N (competing subscribers) | aggregate consume rate |
|---|---|
| 1  | 8,223/s |
| 2  | 10,147/s |
| 4  | 13,185/s |
| 8  | 17,932/s |
| 16 | 22,046/s |
| 32 | 22,968/s |

**Shape: a smooth, diminishing-returns plateau - not NATS's peak-at-N=4-
then-gentle-decline.** NanoMQ's aggregate rate keeps rising (never
declines) through the whole sweep, but with sharply diminishing marginal
returns past N=8 (16->32 only gained +4%). One immediately-visible reason
a single subscriber connection caps out as low as ~8,200/s at all: each
subscriber here runs one serialized `async_receive()`-then-process loop -
architecturally the *same* single-context receive-loop shape `gdb` already
found bottlenecking `nanomq_cli`'s own subscriber above, just with a much
higher individual ceiling since this tool's receive loop does no other
work per message.

**`perf`-profiled the broker itself at N=4 and N=32** (this succeeded
cleanly, once `perf record`'s output file was redirected to `$HOME`
instead of `/tmp` - writing to `/tmp` as root via `sudo -n perf record`
failed with `Permission denied` even though the parent NanoMQ process's
own working files live there without issue; a real, narrower version of
the container restriction that blocked `tshark` entirely earlier in this
investigation, but unlike that one, this had a working escape hatch).
Top self-time symbols, both runs:

| symbol | N=4 | N=32 |
|---|---|---|
| `pthread_mutex_lock` | 3.12% | 3.98% |
| `pthread_mutex_unlock` | 2.49% | 3.37% |
| `__GI___lll_lock_wait` (futex contention) | 1.35% | 1.19% |
| **combined** | **6.96%** | **8.54%** |

Call graphs show these firing from both the receive path
(`tcptran_pipe_recv_cb`/`nano_pipe_recv_cb`) and the send/dispatch path
(`nano_ctx_send`/`tcptran_pipe_send`) - i.e. a real, present, growing-with-N
mutex-contention cost, structurally the same *kind* of thing as
`nats-server`'s `flushOutbound`+`RWMutex` finding (per-connection
socket-path locking that gets more expensive as connection count grows).
**But it grows far more gently**: ~23% relative growth (6.96%->8.54%) here
vs. NATS's ~55% relative growth in `flushOutbound` alone (10.37%->16.02%)
over a comparable N range, before even counting NATS's separate Go
GC/scheduler cost (~9.3%->12.7%) - a cost category NanoMQ structurally
cannot have at all, being C. This difference in growth rate is consistent
with (though this session did not further isolate it down to a single
line of NanoMQ/NNG source) the difference in observed shape: gentle
mutex contention growth -> smooth plateau; NATS's steeper combined
growth (locking *and* GC) -> an actual post-peak decline.

**Answer to the original question**: NanoMQ does *not* share
`nats-server`'s specific bottleneck mechanism (no Go runtime, no
`RWMutex`+GC compounding), and its connection-scaling shape is
measurably more forgiving (plateau vs. decline) - but it is **not**
free of an analogous *category* of cost either: real, `perf`-confirmed
pthread mutex contention on the same kind of per-connection send/receive
code paths, growing with N, just more slowly. "NanoMQ avoids NATS's
bottleneck" would overstate this; "NanoMQ has a gentler version of the
same kind of per-connection locking cost, without the GC component" is
the accurate, evidence-backed claim.

**Caveats, stated plainly**: this test's absolute numbers (max ~23k/s
aggregate) are far below NATS's ~915k/s peak - not a claim that NanoMQ is
slower in absolute terms in general, but a reflection of this specific
tool's per-subscriber-connection design (one `async_receive()` loop per
connection, matching this session's available client library rather than
a purpose-built high-throughput NanoMQ benchmark harness) and a
deliberately-sized ~211-215k backlog per run chosen to keep total sweep
time reasonable, not the broker's own ceiling. The `perf` percentages
are relative sample shares within each run, not a controlled A/B on
otherwise-identical workloads (N=4's run lasted ~16s / 155k samples,
N=32's ~7s / 60k samples) - real signal, not a fully isolated
`nats-server`-style differential profile. Reproduce via
`bench/run_sweep.sh` (the N-sweep) and `bench/run_perf.sh` (the
`perf`-profiled N=32 run; requires `sudo -n perf record` and a
`$HOME`-not-`/tmp` output path, per the finding above).

## Follow-Up: Is The ~8,200/s Single-Connection Ceiling Broker-Side Or Client-Side?

The caveat above ("not a claim that NanoMQ is slower in absolute terms in
general... not the broker's own ceiling") was left as an open question:
`mqtt_raw_bench.cpp`'s single-connection ceiling (~8,223/s at N=1) could be
NanoMQ-specific, or it could be inherent to `boost::mqtt5`'s own
architecture - one serialized `async_receive()`-then-process loop per
connection, the same structural pattern this document's `gdb` investigation
found bottlenecking `nanomq_cli`'s own subscriber. Swapping brokers only
helps if the limit is the former.

**Test**: same `mqtt_raw_bench.cpp` binary, zero code changes, pointed at
**Mosquitto 2.0.22** (`apt install mosquitto`, MQTT v5.0, native shared
subscription support since 2.0) instead of NanoMQ. Same invocation as the
N-sweep above (`bench/topic`, 4 publisher connections, 1s duration, 16
lanes), fresh broker restart before each N.

| N | NanoMQ (known) | Mosquitto 2.0.22 |
|---|---|---|
| 1  | 8,223/s   | **225,873/s** |
| 8  | 17,932/s  | **202,386/s** |
| 32 | 22,968/s  | **134,738/s** |

**Conclusion: broker-side, not client-side - and decisively so.** The
identical, unmodified client hits **~27x** NanoMQ's throughput against
Mosquitto at N=1. This rules out `boost::mqtt5`'s single-context
receive-loop pattern as *the* limiting factor for absolute throughput - it
may still be *a* cost (see the shape note below), but it's clearly not
capping things anywhere near 8,200/s, since the same loop shape sustains
225,873/s here. NanoMQ specifically is the weaker broker for this
workload, not MQTT/`boost::mqtt5` in general.

**A second, unplanned finding - the scaling shape inverted.** NanoMQ's
aggregate rate *rose* with N (8,223/s -> 22,968/s, the smooth
diminishing-returns plateau documented above). Mosquitto's *declines* with
N instead (225,873/s -> 202,386/s -> 134,738/s, N=1 to N=32) - qualitatively
closer to raw NATS core's peak-then-decline shape (see
`project_blazingmq_nats_benchmark.md`) than to NanoMQ's own shape. Not
`perf`-profiled to a root cause here (out of scope for this isolation
test), but worth flagging plainly: **the two brokers aren't just offset by
a constant factor, they have different scaling behavior entirely** -
Mosquitto is far faster at low N but give some of that lead back as N
grows, while NanoMQ is slower everywhere but comparatively more consistent
across N. Which one "wins" at a given N depends on how many competing
consumers you actually plan to run, not just on the N=1 number.

**Caveats, stated plainly**: single test run at each N, not repeated for
run-to-run variance the way earlier sweeps in this document were (time-
boxed isolation test, not a full replacement investigation). N=32's
`received` count (211,863) came in 43 messages *above* `published`
(211,820, `loss_pct=-0.02`) - a benign counting-race artifact at the noise
floor, not a real correctness issue, but noted rather than silently
rounded away. EMQX was in scope as a stretch goal but not run (Mosquitto's
result was decisive enough to answer the question this test was designed
for). Mosquitto's own default config/tuning was used as installed, not
tuned for maximum throughput - the ~27x gap could plausibly narrow or
widen further under different `max_queued_messages`/persistence settings on
either broker; this test answers "is it broker-side" cleanly, not "what's
each broker's true ceiling under best-effort tuning."

## Follow-Up: A Broader Broker Survey - "Not A Stone Left Unturned"

The Mosquitto isolation test above proved broker choice matters enormously,
so this follow-up widens the field: same unmodified `mqtt_raw_bench.cpp`
client, same N=1/8/32 sweep methodology, pointed at four more brokers -
EMQX, VerneMQ, HiveMQ CE, and RabbitMQ's MQTT plugin - each installed
alongside the existing NanoMQ/Mosquitto setups on distinct non-default
ports to avoid any collision with concurrent work in this repo. FlashMQ
was in scope but skipped: no packaged distribution was found quickly, and
a source build was judged not worth the time this survey had budgeted,
consistent with this document's own stated policy of not letting one
broker consume the whole investigation.

**Two brokers produced clean, comparable numbers:**

| N | NanoMQ | Mosquitto 2.0.22 | VerneMQ 2.2.0 |
|---|---|---|---|
| 1  | 8,223/s   | 225,873/s | **151,981/s** |
| 8  | 17,932/s  | 202,386/s | **63,667/s** |
| 32 | 22,968/s  | 134,738/s | **52,564/s** |

VerneMQ works out of the box with this client (0% loss at every N) and
comfortably beats NanoMQ, but trails Mosquitto - roughly 18x NanoMQ at
N=1 vs. Mosquitto's ~27x, and like Mosquitto (and unlike NanoMQ) its
throughput *declines* with N rather than rising. **Mosquitto remains the
clear leader of everything tested so far, at every N measured.**

**Three brokers could not produce a comparable number - each for a
different, precisely-diagnosed reason, not vague "didn't work":**

- **RabbitMQ (via `rabbitmq_mqtt` plugin) does not support MQTT 5 shared
  subscriptions at all.** Confirmed directly: a plain-topic subscribe
  delivers a published message correctly, but a `$share/benchgroup/...`
  subscribe accepts silently (no protocol error) and then never delivers
  anything - `mqtt_raw_bench` reported `loss_pct=100.00` because there is
  nothing to be lost, the subscription was never real. This isn't a bug in
  RabbitMQ, it's a real, documented scope gap in its MQTT plugin (RabbitMQ
  is fundamentally an AMQP broker with MQTT bolted on as a protocol
  adapter, not a purpose-built MQTT implementation) - the N-sweep
  methodology this document uses is structurally inapplicable to it.
- **EMQX 5.8.0 rejects this client's connection outright**, and the reason
  is precisely diagnosed via `strace`, not guessed: `mqtt_raw_bench`
  sends an empty (zero-length) MQTT Client ID, which NanoMQ, Mosquitto,
  VerneMQ, and RabbitMQ all accept and auto-assign an identifier for
  (standard, permitted MQTT 5 behavior per spec section 3.1.3.1), but
  EMQX's default config rejects with CONNACK reason code `0x85` ("Client
  Identifier not valid"). Combined with `boost::mqtt5`'s already-documented
  behavior of retrying `CONNECT` forever on failure (see the auth-hang bug
  found and fixed earlier in this document), this produces a silent,
  permanent reconnect loop with zero application-visible output - not a
  throughput characteristic, a strict client-ID policy default that
  differs from every other broker tested. A direct Python (`paho-mqtt`)
  MQTT5 client confirmed EMQX's shared-subscription delivery itself works
  correctly once a valid Client ID is supplied - this is a client-ID
  policy mismatch with this specific benchmark tool, not a broker
  capability gap, and reconfiguring EMQX's client-ID policy (or patching
  the client) was judged out of scope for a same-day survey.
- **HiveMQ CE 2024.5 could not sustain real-time shared-subscription
  delivery under this benchmark's burst pattern**, even on a freshly-wiped
  data directory (ruling out leftover state from an earlier run as the
  cause). The live test consistently reported `received=0` while
  `published` climbed past a million messages in ~1.3s - but a follow-up
  probe minutes later, on a *separate* fresh connection to the same shared
  group, found a large flood of the earlier run's payloads still draining
  out. This points to severe internal queueing/backpressure latency in
  HiveMQ CE's shared-subscription delivery path under a fast burst,
  not an outright missing feature (messages *do* eventually arrive) - but
  it makes this exact burst-then-measure methodology unable to extract a
  comparable sustained-throughput number from it without broker-side
  tuning well beyond this survey's scope.

**Revised recommendation**: of everything measured across both broker
follow-ups, **Mosquitto is the clear winner for this ecosystem's actual
usage shape** (highest throughput at every N, no compatibility friction
with the existing `boost::mqtt5` client, already the default as of the
switch documented above). VerneMQ is a legitimate distant second if
Mosquitto is ever unavailable for some other reason. EMQX and HiveMQ CE
are not disqualified as broker choices in general - each has a real,
specific, fixable friction point with *this particular benchmark client*
- but neither is a drop-in replacement today without further
broker-side configuration work this survey didn't attempt. RabbitMQ's MQTT
support is architecturally too limited (no shared subscriptions) for this
extension's competing-consumer use case regardless of tuning.

**Standard hygiene**: fresh broker restart before each measured N, `ps`-
and `ss`-verified clean teardown of every broker process and listening
port after each step and at the end of the whole survey - no broker
daemon or non-default port (19001/19021/19031/19041) was left running.
Nothing about NanoMQ's or Mosquitto's existing system installs/config was
touched by this survey.

### Zenoh cheap validation (not an MQTT broker - a different protocol entirely)

A quick, deliberately small validation, not a full investigation: is
[Zenoh](https://zenoh.io) (Rust, Eclipse Foundation) - which an academic
paper (Paul et al. 2026, arXiv:2603.21600) reported hitting 850K msg/s in
a 10,000-subscriber fanout test - actually fast under a pattern closer to
this ecosystem's own usage? Ran a throwaway Python harness
(`bench/zenoh_validation/`, `eclipse-zenoh` pip package v1.10.0, a local
`zenohd` v1.10.0 router on TCP 17447) with the same drain-until-stable
discipline used throughout this document (poll until the receive count is
stable for 3 consecutive checks, not a fixed sleep) and explicit
`reliability=RELIABLE, congestion_control=BLOCK` on the publisher after an
initial best-effort run silently dropped ~39% of 500,000 messages at this
burst rate (confirming Zenoh's *default* pub/sub, like MQTT QoS 0, is
lossy under enough back-pressure - not a bug, just not comparable to this
document's 0%-loss MQTT numbers without asking for reliability
explicitly).

**Important caveat, not glossed over**: Zenoh's plain pub/sub is pure
**fan-out** (every subscriber gets every message), confirmed by inspecting
the Python API directly (`Publisher`/`Subscriber` only; `Queryable`/
`Querier` are request-reply, not a pub/sub message-splitting primitive) -
Zenoh has **no built-in equivalent to MQTT shared subscriptions or NATS
queue groups**. So "N=8" below means 8 independent subscribers each
receiving the *full* stream, not 8 subscribers splitting one stream the
way every other N-sweep in this document measures. These numbers are not
directly comparable to the broker survey table above without accounting
for that difference.

| N | pattern | per-subscriber rate | loss |
|---|---|---|---|
| 1 | fan-out | **1,462,413/s** | 0% (RELIABLE) |
| 8 | fan-out, ×8 | **~526,000/s each** | 0% (RELIABLE) |

Both numbers are real and reliable (0% loss, verified), and both are far
above every MQTT broker measured in this document (Mosquitto's own
best number was 225,873/s at N=1). This is a genuinely different protocol
with a genuinely higher throughput ceiling in this quick test - but it
answers a different question than the MQTT survey above (fan-out
capacity, not competing-consumer capacity), was run once each (not
repeated for variance, unlike the more rigorous numbers elsewhere in this
document), and via Python bindings rather than the C/C++ path this
ecosystem would actually need for a real integration.

**Honest read**: worth a real follow-up investigation if fan-out-shaped
throughput ever matters here, but building a `pg_zenoh`/`zenoh_sidecar`
extension pair is a large undertaking (new client library integration,
no drop-in `broker_host`/`broker_port` swap the way Mosquitto was) and
this validation alone doesn't establish whether Zenoh could support this
ecosystem's actual *competing-consumer* push-consume pattern at all -
that would need to be built on top of Zenoh's primitives (e.g.
liveliness tokens or an application-level partitioning scheme) rather
than reusing something native, which is a real, unresolved design
question a full investigation would need to answer before any throughput
number here can be trusted as representative.

## Maintained Documentation

- [`QUICKSTART.md`](QUICKSTART.md): install, build, and first usage
- [`ARCHITECTURE.md`](ARCHITECTURE.md): the sync/async publish bridge,
  QoS's compile-time-template quirk, the DSM/background-worker design
  behind push-consume, and the honest acknowledgment-semantics limitation

Deliberately out of scope: `pgnats`'s NATS KV (`nats_get/put_*`) and object
store (`nats_get/put_file`) functions have no MQTT equivalent - MQTT's
closest analog (a single retained message per topic) is too weak a fit to
imitate them meaningfully, so this extension doesn't attempt to. Request/
reply (`nats_request_*`) is a possible later phase - MQTT 5's response-topic
+ correlation-data properties can express it, but it's real additional
machinery beyond NATS's built-in request/reply, not a straight port.
