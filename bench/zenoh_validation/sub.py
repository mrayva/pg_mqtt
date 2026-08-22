import zenoh, time, sys, json

sub_id = sys.argv[1]
out_path = sys.argv[2]

count = 0
first_ts = None
last_ts = None
done = False

def on_sample(sample):
    global count, first_ts, last_ts, done
    if bytes(sample.payload.to_bytes()) == b"__END__":
        done = True
        return
    now = time.monotonic()
    if first_ts is None:
        first_ts = now
    last_ts = now
    count += 1

cfg = zenoh.Config()
cfg.insert_json5("mode", '"client"')
cfg.insert_json5("connect/endpoints", '["tcp/127.0.0.1:17447"]')
session = zenoh.open(cfg)
subscriber = session.declare_subscriber("bench/topic", on_sample)

# Poll until count is stable for 3 consecutive 300ms checks, or END marker seen and stable
stable_checks = 0
prev_count = -1
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    time.sleep(0.3)
    if count == prev_count and count > 0:
        stable_checks += 1
    else:
        stable_checks = 0
    prev_count = count
    if stable_checks >= 3:
        break

session.close()

with open(out_path, "w") as f:
    json.dump({
        "sub_id": sub_id,
        "count": count,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "rate": (count / (last_ts - first_ts)) if (first_ts and last_ts and last_ts > first_ts) else None
    }, f)
print(f"sub {sub_id}: received={count}", file=sys.stderr)
