#!/usr/bin/env bash
set -u
cd "$(dirname "$0")"

pkill -9 -f 'nanomq/build/nanomq/nanomq' >/dev/null 2>&1
sleep 1
nohup ~/nanomq/build/nanomq/nanomq start --url nmq-tcp://127.0.0.1:18831 -t 8 -T 8 \
    > /tmp/nanomq_perf.log 2>&1 &
for i in $(seq 1 30); do
    ss -tln 2>/dev/null | grep -q 18831 && break
    sleep 0.2
done

BROKER_PID=$(pgrep -f 'nanomq/build/nanomq/nanomq start' | head -1)
echo "broker pid: $BROKER_PID"

rm -f ~/nanomq_n32.perf.data
sudo -n perf record -F 999 -p "$BROKER_PID" -g -o ~/nanomq_n32.perf.data -- sleep 20 \
    > /tmp/perf_record.log 2>&1 &
PERF_PID=$!

sleep 1
./mqtt_raw_bench 127.0.0.1 18831 bench/topic 32 4 1 16 \
    > /tmp/mqtt_n32_perf_run.out 2> /tmp/mqtt_n32_perf_run.log

wait "$PERF_PID"
echo "perf exit: $?"
cat /tmp/perf_record.log
echo "--- bench result ---"
tail -n 1 /tmp/mqtt_n32_perf_run.out

sudo -n chown "$(id -u):$(id -g)" ~/nanomq_n32.perf.data 2>/dev/null
ls -la ~/nanomq_n32.perf.data 2>/dev/null || echo "no perf data file produced"

pkill -9 -f 'nanomq/build/nanomq/nanomq' >/dev/null 2>&1
echo "done"
