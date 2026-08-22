\echo Use "CREATE EXTENSION pg_mqtt" to load this file. \quit

-- Phase 1 proof-of-linkage only: constructs a real boost::mqtt5::mqtt_client
-- without connecting to a broker, to prove the extension .so actually links
-- and loads into a Postgres backend against the real Boost.MQTT5 dependency
-- chain.
CREATE FUNCTION pg_mqtt_link_check(broker_host text DEFAULT 'localhost', broker_port int DEFAULT 1883)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_mqtt_link_check'
LANGUAGE C STRICT;

-- Phase 2: publish a message to an MQTT broker.
--
-- qos: 0 (at most once), 1 (at least once), 2 (exactly once) - MQTT's own
-- QoS levels. retain: if true, the broker replaces its stored retained
-- message for this topic with this one (delivered immediately to any
-- future subscriber, even if published before they connect).
--
-- Broker address is pg_mqtt.broker_host / pg_mqtt.broker_port (GUCs,
-- default localhost:1883).
CREATE FUNCTION mqtt_publish_binary(
    topic text,
    payload bytea,
    qos int DEFAULT 0,
    retain boolean DEFAULT false
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_binary'
LANGUAGE C;

CREATE FUNCTION mqtt_publish_text(
    topic text,
    payload text,
    qos int DEFAULT 0,
    retain boolean DEFAULT false
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_text'
LANGUAGE C;

CREATE FUNCTION mqtt_publish_json(
    topic text,
    payload json,
    qos int DEFAULT 0,
    retain boolean DEFAULT false
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_json'
LANGUAGE C;

CREATE FUNCTION mqtt_publish_jsonb(
    topic text,
    payload jsonb,
    qos int DEFAULT 0,
    retain boolean DEFAULT false
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_jsonb'
LANGUAGE C;

-- Phase 3: push-consume via a background worker.
--
-- Registers a dedicated background worker that stays subscribed to topic
-- indefinitely, calling callback_fn(payload bytea) for every message
-- received. callback_fn must take exactly one bytea argument; its return
-- value is ignored. It runs in its own transaction per message.
--
-- Important, honest limitation vs pg_blazingmq's bmq_subscribe: this is
-- NOT true at-least-once delivery. Boost.MQTT5's client library handles
-- MQTT's PUBACK/PUBREC/PUBREL/PUBCOMP acknowledgment internally, with no
-- way to defer or control it from application code - so the broker
-- considers a message delivered (and won't redeliver it) regardless of
-- whether callback_fn ever runs or succeeds. If callback_fn raises an
-- error, that transaction is rolled back and a WARNING is logged, but the
-- message is gone either way - QoS here guarantees delivery to the
-- client, not to a successfully-completed callback.
--
-- Returns the worker's PID, which doubles as the subscription handle for
-- mqtt_unsubscribe(worker_pid).
CREATE FUNCTION mqtt_subscribe(
    topic text,
    callback_fn regproc,
    qos int DEFAULT 0
)
RETURNS int
AS 'MODULE_PATHNAME', 'mqtt_subscribe'
LANGUAGE C;

-- Stops a subscriber worker started by mqtt_subscribe(). Validates
-- worker_pid is an active pg_mqtt subscriber (via pg_stat_activity) before
-- signaling it, so this can't be used to terminate arbitrary processes.
CREATE FUNCTION mqtt_unsubscribe(worker_pid int)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_unsubscribe'
LANGUAGE C;
