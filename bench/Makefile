# Standalone raw MQTT load-generator, no PGXS/postgres.h - see
# mqtt_raw_bench.cpp for what this answers and why it exists.

CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread
LDLIBS = -lpthread

mqtt_raw_bench: mqtt_raw_bench.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f mqtt_raw_bench

.PHONY: clean
