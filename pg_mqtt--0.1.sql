\echo Use "CREATE EXTENSION pg_mqtt" to load this file. \quit

-- Phase 1 proof-of-linkage only: constructs a real boost::mqtt5::mqtt_client
-- without connecting to a broker, to prove the extension .so actually links
-- and loads into a Postgres backend against the real Boost.MQTT5 dependency
-- chain.
CREATE FUNCTION pg_mqtt_link_check(broker_host text DEFAULT 'localhost', broker_port int DEFAULT 1883)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_mqtt_link_check'
LANGUAGE C STRICT;
