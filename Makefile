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
