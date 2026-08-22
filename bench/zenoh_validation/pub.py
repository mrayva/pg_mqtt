import zenoh, time, sys

N_MSGS = int(sys.argv[1]) if len(sys.argv) > 1 else 500000
PAYLOAD = b"x" * 64

cfg = zenoh.Config()
cfg.insert_json5("mode", '"client"')
cfg.insert_json5("connect/endpoints", '["tcp/127.0.0.1:17447"]')
session = zenoh.open(cfg)
publisher = session.declare_publisher(
    "bench/topic",
    congestion_control=zenoh.CongestionControl.BLOCK,
    reliability=zenoh.Reliability.RELIABLE,
)

start = time.monotonic()
for i in range(N_MSGS):
    publisher.put(PAYLOAD)
elapsed = time.monotonic() - start
print(f"published {N_MSGS} in {elapsed:.3f}s ({N_MSGS/elapsed:.0f}/s publish-side)", file=sys.stderr)

# give subscribers a moment then send END marker a few times for reliability
time.sleep(0.5)
for _ in range(5):
    publisher.put(b"__END__")
    time.sleep(0.05)

time.sleep(0.5)
session.close()
