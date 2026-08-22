-- Phase 1: proof-of-linkage. Constructs a real boost::mqtt5::mqtt_client
-- without calling async_run(), so no live broker is actually required for
-- this one - it's here for completeness/ordering, not because it needs the
-- test broker manage_broker.sh starts.
CREATE EXTENSION pg_mqtt;
SELECT pg_mqtt_link_check('localhost', 18830);
