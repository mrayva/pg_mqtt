#!/usr/bin/env bash
#
# Starts/stops a scratch Mosquitto broker for `make test` - the primary,
# recommended broker as of 2026-08-22 (see README.md's "Broker choice"
# section: an isolation test found NanoMQ's raw single-connection
# throughput ceiling, ~8,200/s, is a NanoMQ-specific weakness, not an
# MQTT-protocol or Boost.MQTT5-client one - Mosquitto hit ~27x more with
# the identical client). NanoMQ remains supported and documented as an
# alternative - see manage_broker_nanomq.sh.
#
# Unlike NanoMQ (global control - one broker process on the whole machine
# at a time), Mosquitto is a genuinely per-instance-scoped daemon: each
# invocation gets its own scratch dir, config, PID file, and three
# listeners (plain/auth-required/TLS) on non-default ports, so multiple
# independent scratch brokers *could* coexist if ever needed - a real
# improvement over NanoMQ's scratch-broker story, not just a port change.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH_DIR="$SCRIPT_DIR/.broker_scratch"
CONF_FILE="$SCRATCH_DIR/mosquitto.conf"
PID_FILE="$SCRATCH_DIR/mosquitto.pid"
BROKER_HOST="127.0.0.1"
PLAIN_PORT="18830"
AUTH_PORT="18832"
TLS_PORT="18831"
AUTH_USER="testuser"
AUTH_PASS="testpass123"

cmd="${1:-}"

start() {
    if ss -tln 2>/dev/null | grep -q ":$PLAIN_PORT "; then
        echo "manage_broker.sh: something is already listening on port $PLAIN_PORT" >&2
        return 0
    fi

    if ! command -v mosquitto >/dev/null 2>&1; then
        echo "manage_broker.sh: mosquitto not found on PATH (apt install mosquitto)" >&2
        exit 1
    fi

    rm -rf "$SCRATCH_DIR"
    mkdir -p "$SCRATCH_DIR"

    # Reuse the same self-signed cert this test suite has always used for
    # TLS verification, if present; otherwise generate a fresh one - either
    # way this is a throwaway scratch cert, not for any real deployment.
    if [ -f "$SCRIPT_DIR/tls/server_cert.pem" ]; then
        cp "$SCRIPT_DIR/tls/server_cert.pem" "$SCRATCH_DIR/server_cert.pem"
        cp "$SCRIPT_DIR/tls/server_key.pem" "$SCRATCH_DIR/server_key.pem"
    else
        openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
            -keyout "$SCRATCH_DIR/server_key.pem" -out "$SCRATCH_DIR/server_cert.pem" \
            -subj "/CN=localhost" >/dev/null 2>&1
        mkdir -p "$SCRIPT_DIR/tls"
        cp "$SCRATCH_DIR/server_cert.pem" "$SCRIPT_DIR/tls/server_cert.pem"
        cp "$SCRATCH_DIR/server_key.pem" "$SCRIPT_DIR/tls/server_key.pem"
    fi

    mosquitto_passwd -c -b "$SCRATCH_DIR/passwd" "$AUTH_USER" "$AUTH_PASS" >/dev/null 2>&1

    sed \
        -e "s|SCRATCH_DIR|$SCRATCH_DIR|g" \
        -e "s|PLAIN_PORT|$PLAIN_PORT|" \
        -e "s|AUTH_PORT|$AUTH_PORT|" \
        -e "s|TLS_PORT|$TLS_PORT|" \
        "$SCRIPT_DIR/mosquitto.conf.template" > "$CONF_FILE"

    mosquitto -c "$CONF_FILE" -d > "$SCRATCH_DIR/start.out" 2>&1 || {
        echo "manage_broker.sh: mosquitto start failed - see $SCRATCH_DIR/start.out" >&2
        exit 1
    }

    for _ in $(seq 1 50); do
        if ss -tln 2>/dev/null | grep -q ":$PLAIN_PORT "; then
            echo "manage_broker.sh: broker started (plain=127.0.0.1:$PLAIN_PORT auth=127.0.0.1:$AUTH_PORT tls=127.0.0.1:$TLS_PORT, user=$AUTH_USER pass=$AUTH_PASS)"
            return 0
        fi
        sleep 0.2
    done

    echo "manage_broker.sh: broker did not start within 10s - see $SCRATCH_DIR/mosquitto.log" >&2
    exit 1
}

stop() {
    if [ -f "$PID_FILE" ]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    pkill -f "mosquitto -c $CONF_FILE" 2>/dev/null || true

    for _ in $(seq 1 50); do
        ss -tln 2>/dev/null | grep -q ":$PLAIN_PORT " || break
        sleep 0.2
    done
    echo "manage_broker.sh: broker stopped"
}

case "$cmd" in
    start) start ;;
    stop) stop ;;
    *) echo "usage: $0 {start|stop}" >&2; exit 1 ;;
esac
