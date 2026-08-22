# pg_mqtt Makefile
#
# Phase 1: proof-of-linkage build only (pg_mqtt_link_check()). Links against
# the system Boost 1.88+ install, which already ships boost::mqtt5
# (formerly Async.MQTT5) - confirmed via /usr/include/boost/mqtt5.hpp - no
# vendoring needed, unlike pg_blazingmq's BDE/NTF/bmq dependency chain.

MODULE_big = pg_mqtt
OBJS = pg_mqtt.o

EXTENSION = pg_mqtt
DATA = pg_mqtt--0.1.sql pg_mqtt--0.2.sql pg_mqtt--0.3.sql \
       pg_mqtt--0.1--0.2.sql pg_mqtt--0.2--0.3.sql

REGRESS = 01_link_check 02_publish 03_subscribe

PG_CPPFLAGS = -std=c++17 -fPIC
# Modern Boost.System (1.69+) is header-only by default - no libboost_system
# to link against on this system (confirmed: -lboost_system fails to find
# the library at all).
SHLIB_LINK = -lstdc++ -lpthread

# Use C++ compiler
CC = g++
CXX = g++

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS links MODULE_big with the C driver; avoid passing C-only warning flags
# from PostgreSQL's build into that link. C++ compilation uses CXXFLAGS below.
override CFLAGS :=

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

# `make installcheck` (PGXS's own target, above) assumes a broker is already
# running on 127.0.0.1:18830 and pg_mqtt is already `make install`'d. `make
# test` is the one-command version: starts a scratch broker, runs
# installcheck, always stops the broker after - even if installcheck fails,
# so a failing test run doesn't leak a background broker process.
.PHONY: test
test:
	test/manage_broker.sh start
	$(MAKE) installcheck; status=$$?; \
	test/manage_broker.sh stop; \
	exit $$status
