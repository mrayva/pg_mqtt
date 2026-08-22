\echo Use "ALTER EXTENSION pg_mqtt UPDATE TO '0.4'" to load this file. \quit

-- 0.4: message_expiry_seconds and user_properties on the publish path.
-- See pg_mqtt--0.4.sql for the full doc comments, and README.md for the
-- new connect-time GUCs (auth, Last Will and Testament, session
-- persistence, TLS) added in this version - those are runtime config, not
-- SQL objects, so there's nothing to CREATE for them here.
--
-- The old 4-arg signatures must be dropped explicitly before creating the
-- new 6-arg ones: CREATE OR REPLACE FUNCTION with a different parameter
-- list creates a second, distinct overload rather than replacing the
-- original (Postgres identifies a function by name+arg-types together) -
-- reproduced live, `ALTER EXTENSION pg_mqtt UPDATE` without these DROPs
-- left both the old and new mqtt_publish_text overloads registered
-- simultaneously, making any 4-arg call ambiguous ("not unique") and,
-- worse, silently invoking the new 6-arg C function body with the old
-- 4-arg fcinfo via the *other* still-callable overload path in one
-- combination, reading past the actual argument array (a segfault in
-- pg_detoast_datum on garbage arg 5, confirmed via gdb backtrace).
DROP FUNCTION IF EXISTS mqtt_publish_binary(text, bytea, int, boolean);
DROP FUNCTION IF EXISTS mqtt_publish_text(text, text, int, boolean);
DROP FUNCTION IF EXISTS mqtt_publish_json(text, json, int, boolean);
DROP FUNCTION IF EXISTS mqtt_publish_jsonb(text, jsonb, int, boolean);

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
