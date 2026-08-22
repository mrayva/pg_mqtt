#!/usr/bin/env bash
# N-sweep driver for mqtt_raw_bench against a fresh NanoMQ broker restart
# per N (methodology: back-to-back runs against the same already-running
# broker degrade ~2x, per this session's earlier BlazingMQ finding).
set -u
cd "$(dirname "$0")"

RESULTS=/tmp/mqtt_sweep_results.txt
: > "$RESULTS"

restart_broker() {
    pkill -9 -f 'nanomq/build/nanomq/nanomq' >/dev/null 2>&1
    sleep 1
    nohup ~/nanomq/build/nanomq/nanomq start --url nmq-tcp://127.0.0.1:18831 -t 8 -T 8 \
        > /tmp/nanomq_bench.log 2>&1 &
    for i in $(seq 1 30); do
        if ss -tln 2>/dev/null | grep -q 18831; then
            return 0
        fi
        sleep 0.2
    done
    echo "broker failed to start" >&2
    return 1
}

for N in 1 2 4 8 16 32; do
    echo "=== N=$N ===" | tee -a "$RESULTS"
    if ! restart_broker; then
        echo "SKIP N=$N (broker restart failed)" | tee -a "$RESULTS"
        continue
    fi
    sleep 0.5
    ./mqtt_raw_bench 127.0.0.1 18831 bench/topic "$N" 4 1 16 \
        > /tmp/mqtt_sweep_N${N}.out 2> /tmp/mqtt_sweep_N${N}.log
    tail -n 1 /tmp/mqtt_sweep_N${N}.out | tee -a "$RESULTS"
done

pkill -9 -f 'nanomq/build/nanomq/nanomq' >/dev/null 2>&1
echo "=== DONE ===" | tee -a "$RESULTS"
