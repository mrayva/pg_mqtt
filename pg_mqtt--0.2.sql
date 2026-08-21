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
