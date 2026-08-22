# Quick Start

Assumes Boost 1.88+ (already ships `boost::mqtt5`, no vendoring needed) and
a running MQTT broker - [NanoMQ](https://nanomq.io/) is the target for
development/testing, already installed on this machine at
`/usr/local/bin/nanomq`.

## Build And Enable

```bash
make
sudo make install
psql -d postgres -c 'CREATE EXTENSION pg_mqtt'
```

## Start A Broker

For local testing, `test/manage_broker.sh` starts/stops a scratch NanoMQ
broker on `127.0.0.1:18830` (deliberately not MQTT's default 1883, to avoid
clashing with any real broker already running):

```bash
test/manage_broker.sh start
```

(`make test` does this automatically around `make installcheck` - see
README.md's "Testing" section. NanoMQ's own start/stop control is global,
not per-instance - only one broker can be managed this way on a machine at
a time.)

## Publish

```sql
SET pg_mqtt.broker_host = 'localhost';
SET pg_mqtt.broker_port = 18830;

SELECT mqtt_publish_text('sensors/room1/temp', '21.5', 1, false);
SELECT mqtt_publish_jsonb('sensors/room1/status', '{"online": true}'::jsonb, 1, true);
```

`qos` (third argument, default 0) is MQTT's own delivery guarantee - 0
(fire-and-forget), 1 (`PUBACK`-acknowledged), or 2 (full four-packet
handshake). `retain` (fourth argument, default false), if true, keeps the
message as the topic's retained value - any future subscriber gets it
immediately on connecting, even long after this call returns.

## Push-Consume

```sql
CREATE TABLE received_messages (topic_hint text, payload text, received_at timestamptz DEFAULT now());

CREATE FUNCTION handle_message(payload bytea) RETURNS void AS $$
BEGIN
  INSERT INTO received_messages (topic_hint, payload) VALUES ('sensors', convert_from(payload, 'UTF8'));
END;
$$ LANGUAGE plpgsql;

-- Unlike pg_blazingmq's bmq_subscribe, the worker captures the calling
-- backend's *current* broker_host/broker_port at mqtt_subscribe() time -
-- a plain session-local SET is enough here, no ALTER DATABASE needed.
SET pg_mqtt.broker_host = 'localhost';
SET pg_mqtt.broker_port = 18830;

SELECT mqtt_subscribe('sensors/+/temp', 'handle_message'::regproc, 1) AS worker_pid;
--  worker_pid
-- ------------
--      901816

-- ... messages arrive asynchronously, with no further action from this session ...

SELECT mqtt_unsubscribe(901816);  -- stops the worker cleanly
```

**Read before relying on this for anything important**: `mqtt_subscribe`'s
delivery guarantee is weaker than `pg_blazingmq`'s `bmq_subscribe`. MQTT's
QoS handshake happens inside the client library itself, with no way for
this extension to defer it - so a callback that fails is logged as a
`WARNING` and the transaction rolls back, but the message itself is *not*
redelivered. See README.md's Phase 3 section and
[ARCHITECTURE.md](ARCHITECTURE.md) for the full explanation.

See README.md for the full function reference and
[ARCHITECTURE.md](ARCHITECTURE.md) for the internal design.
