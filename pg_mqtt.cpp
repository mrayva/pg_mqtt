/*
 * pg_mqtt.cpp
 *
 * Phase 1: pg_mqtt_link_check() - proof-of-linkage only, see README.
 * Constructs a real boost::mqtt5::mqtt_client (touching the real Boost.MQTT5
 * dependency chain) without connecting to a broker, so no live broker is
 * required for this one - it proves the extension's .so actually links and
 * loads inside a Postgres backend, nothing more.
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "varatt.h"

PG_MODULE_MAGIC;
}

#include <boost/mqtt5.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <sstream>
#include <stdexcept>
#include <string>

namespace mqtt5 = boost::mqtt5;
namespace asio = boost::asio;

extern "C" {

PG_FUNCTION_INFO_V1(pg_mqtt_link_check);

Datum pg_mqtt_link_check(PG_FUNCTION_ARGS)
{
    text* host_text = PG_GETARG_TEXT_PP(0);
    std::string host(VARDATA_ANY(host_text), VARSIZE_ANY_EXHDR(host_text));
    int32 port = PG_GETARG_INT32(1);

    if (port <= 0 || port > 65535) {
        ereport(ERROR, (errmsg("port must be between 1 and 65535")));
    }

    try {
        asio::io_context ioc;
        mqtt5::mqtt_client<asio::ip::tcp::socket> client(ioc);
        client.brokers(host, static_cast<uint16_t>(port));

        std::ostringstream out;
        out << "pg_mqtt link OK: broker=" << host << ":" << port;
        PG_RETURN_TEXT_P(cstring_to_text(out.str().c_str()));
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errmsg("pg_mqtt_link_check failed"),
                 errdetail("%s", ex.what())));
    }

    PG_RETURN_NULL(); // unreachable
}

} // extern "C"
