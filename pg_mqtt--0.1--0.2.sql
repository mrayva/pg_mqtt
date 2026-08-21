\echo Use "ALTER EXTENSION pg_mqtt UPDATE TO '0.2'" to load this file. \quit

-- 0.2: publish path. See pg_mqtt--0.2.sql for the full doc comments.
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
