#!/usr/bin/env bash
#
# Starts/stops a scratch NanoMQ broker for `make test`.
#
# NanoMQ's start/stop control is global (a single PID file at
# /tmp/nanomq/nanomq.pid and a fixed control IPC socket at
# /tmp/nanomq_cmd.ipc - confirmed by observation, not documented anywhere
# obvious), not per-instance the way BlazingMQ's broker is - so only one
# NanoMQ instance can be managed this way on a given machine at a time.
# Fine for this test suite (one scratch broker, like pg_blazingmq's own
# manage_broker.sh), but don't assume you can run two independent test
# brokers side by side with this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH_DIR="$SCRIPT_DIR/.broker_scratch"
LOG_FILE="$SCRATCH_DIR/broker.log"
BROKER_HOST="127.0.0.1"
BROKER_PORT="18830"

cmd="${1:-}"

start() {
    if ss -tln 2>/dev/null | grep -q ":$BROKER_PORT "; then
        echo "manage_broker.sh: something is already listening on port $BROKER_PORT" >&2
        return 0
    fi

    if ! command -v nanomq >/dev/null 2>&1; then
        echo "manage_broker.sh: nanomq not found on PATH" >&2
        echo "  (see README's Building section)" >&2
        exit 1
    fi

    rm -rf "$SCRATCH_DIR"
    mkdir -p "$SCRATCH_DIR"

    nanomq start --url "nmq-tcp://$BROKER_HOST:$BROKER_PORT" -d --log_file "$LOG_FILE" \
        > "$SCRATCH_DIR/start.out" 2>&1 || {
        echo "manage_broker.sh: nanomq start failed - see $SCRATCH_DIR/start.out" >&2
        exit 1
    }

    for _ in $(seq 1 50); do
        if ss -tln 2>/dev/null | grep -q ":$BROKER_PORT "; then
            echo "manage_broker.sh: broker started (127.0.0.1:$BROKER_PORT)"
            return 0
        fi
        sleep 0.2
    done

    echo "manage_broker.sh: broker did not start within 10s - see $SCRATCH_DIR/start.out" >&2
    exit 1
}

stop() {
    nanomq stop > /dev/null 2>&1 || true

    for _ in $(seq 1 50); do
        ss -tln 2>/dev/null | grep -q ":$BROKER_PORT " || break
        sleep 0.2
    done
    echo "manage_broker.sh: broker stopped"
}

case "$cmd" in
    start) start ;;
    stop) stop ;;
    *) echo "usage: $0 {start|stop}" >&2; exit 1 ;;
esac
