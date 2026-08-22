\echo Use "ALTER EXTENSION pg_mqtt UPDATE TO '0.3'" to load this file. \quit

-- 0.3: push-consume. See pg_mqtt--0.3.sql for the full doc comment on the
-- honest ack-semantics limitation vs pg_blazingmq's bmq_subscribe.
CREATE FUNCTION mqtt_subscribe(
    topic text,
    callback_fn regproc,
    qos int DEFAULT 0
)
RETURNS int
AS 'MODULE_PATHNAME', 'mqtt_subscribe'
LANGUAGE C;

CREATE FUNCTION mqtt_unsubscribe(worker_pid int)
RETURNS boolean
AS 'MODULE_PATHNAME', 'mqtt_unsubscribe'
LANGUAGE C;
