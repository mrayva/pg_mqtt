# Architecture

## Overview

`pg_mqtt` is a single-file PostgreSQL C++ extension (PGXS-based, mirroring
`pg_blazingmq`'s/`pg_zerialize`'s build conventions) that links Boost's
MQTT5 client (`boost::mqtt5::mqtt_client`, formerly the standalone
Async.MQTT5 library, merged into Boost 1.88+) directly into the Postgres
backend. The SQL surface is deliberately modeled on `pgnats`'s function
shape for consistency across this workspace's messaging extensions - the
implementation underneath is unrelated.

Boost.MQTT5's `mqtt_client` is a raw Asio object with no self-contained
threading model of its own (unlike BlazingMQ's `bmqa::Session`, which
manages its own I/O internally) - nothing happens, including keeping the
connection alive, unless its `io_context` is pumped continuously. This one
fact shapes almost every design decision below.

## Publish: Session Lifecycle And The Sync/Async Bridge

- One `boost::mqtt5::mqtt_client` per backend process, lazily created on
  first use (`get_client()`) and torn down via `on_proc_exit` - same shape
  as `pg_blazingmq.cpp`'s `get_session()`.
- Because the client needs continuous pumping, `get_client()` also starts a
  dedicated background `std::thread` running `ioc.run()` for the client's
  whole lifetime, and kicks off `async_run()` once via a detached
  completion token. `async_run()` is itself long-lived - its documented
  completion condition is "cancelled, disconnected, or a fatal error", not
  "connected" - so it must never be awaited synchronously; it's the
  operation *keeping* the connection alive, not a one-shot connect step.
- Each SQL-level publish call (`publish_sync()`) is a synchronous wrapper
  around one async operation: `asio::post()` the real `async_publish` call
  onto the client's `io_context`, then block the *calling* (backend)
  thread on a `std::promise`/`future` until the completion handler - which
  runs on the *background* thread - resolves it. This is the standard
  synchronous-wrapper-around-async-library pattern, and the safety of
  capturing the promise by reference in the handler depends entirely on
  the calling thread genuinely blocking on the future before returning -
  it does, so the stack frame outlives the handler's execution.

## QoS Is A Compile-Time Parameter

Boost.MQTT5's `async_publish` is `template <qos_e qos_type, ...>` - QoS is
selected at compile time, not passed as a runtime argument, and the three
QoS levels don't even share a completion handler signature (QoS 0
completes with just an error code; QoS 1/2 also carry a reason code and
properties from the PUBACK/PUBCOMP packet). `publish_sync()` handles this
with a runtime `if`/`else if` over the SQL-supplied `qos int`, each branch
instantiating a different `async_publish<qos_e::...>` - three near-duplicate
branches, kept simple rather than genericized further for this first cut,
since a generic handler lambda (ignoring the extra QoS 1/2 parameters via
`auto&&...`) already unifies the completion side.

## Push-Consume: Background Worker + DSM Handoff

Mirrors `pg_blazingmq.cpp`'s `bmq_subscribe`/`bmq_subscriber_main`
architecture directly - the hard parts (dynamic background worker
registration, one-shot config handoff, a readiness handshake, PID-as-handle)
were already solved there and ported over largely unchanged:

- **Config handoff.** A fixed-size `SubscriberConfig` struct (topic, QoS,
  callback OID, broker host/port, target database/role) is written into a
  pinned DSM segment and handed to a freshly `RegisterDynamicBackgroundWorker`'d
  worker via `bgw_main_arg`.
- **Readiness handshake.** `WaitForBackgroundWorkerStartup()` only proves
  the OS process started, not that `async_subscribe()` has actually
  completed (a real network round trip - the worker's own single-threaded
  loop drives `ioc.run_one()` until its subscribe completion handler
  fires). A `pg_atomic_uint32 ready` field in the same DSM segment closes
  this exactly like `pg_blazingmq`'s does: the worker flips it after a
  successful SUBACK, `mqtt_subscribe()` polls it (bounded 5s, best-effort -
  a slow-to-connect worker still gets its PID back, with a `WARNING`
  instead of a hard failure, since it keeps retrying independently either
  way).
- **No separate registry.** The worker's PID doubles as the subscription
  handle via `pg_stat_activity.backend_type = 'pg_mqtt subscriber'` -
  `mqtt_unsubscribe()` checks this before signaling a PID, so it can't be
  used to kill arbitrary processes.
- **Per-message dispatch.** Each message runs in its own transaction
  (`StartTransactionCommand`/`SPI_connect`/`PushActiveSnapshot`, then
  `OidFunctionCall1(callback_fn, ...)` inside `PG_TRY`/`PG_CATCH`). On
  success, commit. On failure: roll back, log a `WARNING`, and move on -
  see the next section for why this is *not* the same at-least-once
  guarantee `bmq_subscriber_main` provides.
- **GUC scoping is genuinely different from `pg_blazingmq` here, not just
  a naming variation.** `mqtt_subscribe()` copies the *calling backend's
  current* `pg_mqtt.broker_host`/`broker_port` values into the DSM config
  at subscribe time - the worker never reads GUC state itself. So a plain
  session-local `SET` before calling `mqtt_subscribe()` is sufficient; no
  `ALTER DATABASE` is needed the way `pg_blazingmq`'s `bmq_subscribe`
  requires (there, the worker is a separate process that reads the
  *database's* GUC default itself, since a session-local `SET` never
  reaches a bgworker's own process-local GUC state). Both approaches solve
  the same underlying problem (a bgworker is a separate OS process with
  independent GUC state); `pg_mqtt` just solves it by capturing the value
  once at handoff time instead of having the worker read it fresh.

## The Real Acknowledgment-Semantics Limitation

This is the most important architectural difference from `pg_blazingmq`,
and it's a hard limit of the underlying library, not a design choice this
extension could revisit: grepping the entire `boost/mqtt5` header tree
turns up no manual-ack API anywhere. `async_receive()`'s own documentation
states the client "receive[s] and complete[s] deliveries for all PUBLISH
packets... throughout its lifetime" into internal storage - meaning
PUBACK/PUBREC/PUBREL/PUBCOMP handshaking happens inside the client library
itself, unconditionally, independent of whether the application (this
extension's SQL callback) ever runs or succeeds.

Practically: `bmq_subscriber_main` only calls `session.confirmMessage()`
*after* the callback's transaction commits, so a failing callback leaves
the message unconfirmed and BlazingMQ redelivers it - genuine at-least-once
all the way to a successfully-processed callback. `mqtt_subscriber_main`
has no equivalent lever to pull; the broker already considers the message
delivered by the time the callback even starts running. A failed callback
here means the data is gone, not retried - QoS in this extension guarantees
delivery *to the client*, not *to a successfully-completed callback*.

## A Real Bug, Preserved As A Cautionary Note

While verifying the callback-failure path (not just the happy path) during
Phase 3, building the `WARNING` message directly from `edata->message`
*after* `FlushErrorState()`/`AbortCurrentTransaction()` had already run
corrupted the heap - a `pfree` on an invalid pointer, with the freed
chunk's header overwritten by fragments of that same message text.
Reproduced consistently; confirmed via a diagnostic build using a static
message instead, which didn't crash. `CopyErrorData()` is documented to
make the returned `ErrorData` independent of `FlushErrorState()`, but
empirically its *message buffer* did not survive `AbortCurrentTransaction()`
intact in this code path. Fixed by copying `edata->message` into an
independent `std::string` immediately after `CopyErrorData()`, before
`FlushErrorState()`/`AbortCurrentTransaction()` run - not by relying on
`edata`'s own claimed independence past that point. Verified fixed across
repeated consecutive callback failures.

## Build

Boost 1.88+ ships `boost::mqtt5` already - no vendoring, unlike
`pg_blazingmq`'s BDE/NTF/bmq chain. Modern Boost.System (1.69+) is
header-only by default, so there's no `libboost_system` to link against on
this system.
