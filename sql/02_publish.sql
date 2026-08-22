-- Phase 2: mqtt_publish_binary/text/json/jsonb(). Needs a live broker
-- (manage_broker.sh). Content/retain verification happens for real in
-- 03_subscribe.sql via a callback that actually receives what gets
-- published here - this file only checks the calls succeed/fail correctly,
-- since Phase 2 alone has no pull-consume path to read messages back.
SET pg_mqtt.broker_host = 'localhost';
SET pg_mqtt.broker_port = 18830;

-- Happy path: all four payload variants, QoS 0/1/2, retain both ways.
SELECT mqtt_publish_binary('pgregress/publish/binary', '\x0102030405'::bytea, 0, false);
SELECT mqtt_publish_binary('pgregress/publish/binary', '\x0102030405'::bytea, 1, false);
SELECT mqtt_publish_binary('pgregress/publish/binary', '\x0102030405'::bytea, 2, true);

SELECT mqtt_publish_text('pgregress/publish/text', 'hello mqtt', 1, false);
SELECT mqtt_publish_json('pgregress/publish/json', '{"a":1,"b":"two"}'::json, 1, false);
SELECT mqtt_publish_jsonb('pgregress/publish/jsonb', '{"a":1,"b":"two"}'::jsonb, 1, true);

-- Negative path: null topic/payload.
SELECT mqtt_publish_text(NULL, 'x');
SELECT mqtt_publish_text('pgregress/publish/text', NULL);

-- Negative path: qos out of range - must error clearly, before any network
-- call succeeds silently with a clamped value.
SELECT mqtt_publish_text('pgregress/publish/text', 'x', 3, false);
SELECT mqtt_publish_text('pgregress/publish/text', 'x', -1, false);
