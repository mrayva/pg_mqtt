\echo Use "ALTER EXTENSION pg_mqtt UPDATE TO '0.4'" to load this file. \quit

-- 0.4: message_expiry_seconds and user_properties on the publish path.
-- See pg_mqtt--0.4.sql for the full doc comments, and README.md for the
-- new connect-time GUCs (auth, Last Will and Testament, session
-- persistence, TLS) added in this version - those are runtime config, not
-- SQL objects, so there's nothing to CREATE for them here.
CREATE OR REPLACE FUNCTION mqtt_publish_binary(
    topic text,
    payload bytea,
    qos int DEFAULT 0,
    retain boolean DEFAULT false,
    message_expiry_seconds int DEFAULT NULL,
    user_properties jsonb DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_binary'
LANGUAGE C;

CREATE OR REPLACE FUNCTION mqtt_publish_text(
    topic text,
    payload text,
    qos int DEFAULT 0,
    retain boolean DEFAULT false,
    message_expiry_seconds int DEFAULT NULL,
    user_properties jsonb DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_text'
LANGUAGE C;

CREATE OR REPLACE FUNCTION mqtt_publish_json(
    topic text,
    payload json,
    qos int DEFAULT 0,
    retain boolean DEFAULT false,
    message_expiry_seconds int DEFAULT NULL,
    user_properties jsonb DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_json'
LANGUAGE C;

CREATE OR REPLACE FUNCTION mqtt_publish_jsonb(
    topic text,
    payload jsonb,
    qos int DEFAULT 0,
    retain boolean DEFAULT false,
    message_expiry_seconds int DEFAULT NULL,
    user_properties jsonb DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'mqtt_publish_jsonb'
LANGUAGE C;
