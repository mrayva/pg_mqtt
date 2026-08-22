// mqtt_raw_bench.cpp
//
// Raw (non-Postgres, non-nanomq_cli) MQTT load generator, built directly
// against boost::mqtt5 - the same client library pg_mqtt.cpp uses - to
// answer the question nanomq_cli was proven unfit to answer: does NanoMQ's
// aggregate consume rate under a competing-consumer group (MQTT 5 shared
// subscription, the analog of a NATS queue group) scale the same way raw
// NATS core did (peak at N=4 ~915k/s, then a gentle decline through N=32,
// root-caused to nats-server's per-connection flushOutbound)?
//
// Usage: mqtt_raw_bench <host> <port> <topic> <num_subscribers>
//                        <num_publisher_conns> <duration_sec>
//                        [lanes_per_publisher]
//
// Publishers publish QoS 0 to <topic>. Subscribers each open their own
// connection and subscribe to "$share/benchgroup/<topic>" (competing
// consumption - see pg_mqtt.cpp's no_local fix for $share/ topics, mirrored
// here or NanoMQ rejects the subscribe outright). Each publisher connection
// runs `lanes` concurrent self-chaining async_publish loops (pipelining,
// since QoS 0's completion fires on local write-buffer handoff, not a
// broker round trip - a single serial chain undercounts achievable rate).
//
// Publishers run for duration_sec then stop issuing new publishes.
// Subscribers keep running until the received-count stops climbing across
// 3 consecutive 200ms samples (drain-to-steady-state), so the reported
// aggregate rate reflects genuine sustained throughput, not an
// undrained-backlog burst (see project_blazingmq_nats_benchmark.md
// methodology lesson 0 - this exact mistake was made and caught earlier
// this session in the BlazingMQ investigation).

#include <boost/mqtt5.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mqtt5 = boost::mqtt5;
namespace asio = boost::asio;
using tcp_client = mqtt5::mqtt_client<asio::ip::tcp::socket>;

static std::atomic<bool> g_stop_publishing{false};
static std::atomic<uint64_t> g_published{0};
static std::atomic<uint64_t> g_received{0};

struct Subscriber {
    asio::io_context ioc;
    std::unique_ptr<tcp_client> client;
    std::atomic<bool> ready{false};
    std::atomic<bool> stop{false};
    std::thread thr;
};

struct Publisher {
    asio::io_context ioc;
    std::unique_ptr<tcp_client> client;
    std::thread thr;
};

static void subscriber_run(Subscriber* s, const std::string& host, int port,
                            const std::string& share_topic)
{
    s->client = std::make_unique<tcp_client>(s->ioc);
    s->client->brokers(host, static_cast<uint16_t>(port));
    s->client->async_run(asio::detached);

    // Force no_local off for $share/ topics - MQTT 5 3.8.3.1 makes it a
    // protocol error otherwise; NanoMQ correctly rejects it (see
    // pg_mqtt.cpp's mqtt_subscriber_main, commit feade9b).
    mqtt5::subscribe_topic sub_topic{
        share_topic,
        mqtt5::subscribe_options{mqtt5::qos_e::at_most_once, mqtt5::no_local_e::no}};

    bool sub_done = false;
    boost::system::error_code sub_ec;
    s->client->async_subscribe(
        std::vector<mqtt5::subscribe_topic>{sub_topic}, mqtt5::subscribe_props{},
        [&](boost::system::error_code ec, std::vector<mqtt5::reason_code>, mqtt5::suback_props) {
            sub_ec = ec;
            sub_done = true;
        });
    while (!sub_done) s->ioc.run_one();
    if (sub_ec) {
        std::fprintf(stderr, "subscribe failed: %s\n", sub_ec.message().c_str());
        std::exit(1);
    }
    s->ready.store(true);

    std::function<void()> recv_loop = [&]() {
        if (s->stop.load()) return;
        s->client->async_receive(
            [s, &recv_loop](boost::system::error_code ec, std::string, std::string,
                             mqtt5::publish_props) {
                if (!ec) g_received.fetch_add(1, std::memory_order_relaxed);
                if (!s->stop.load()) recv_loop();
            });
    };
    recv_loop();

    while (!s->stop.load()) {
        s->ioc.run_one_for(std::chrono::milliseconds(200));
    }
    s->client->async_disconnect(asio::detached);
    s->ioc.run_for(std::chrono::milliseconds(200));
}

static void publisher_run(Publisher* p, const std::string& host, int port,
                           const std::string& topic, int lanes)
{
    p->client = std::make_unique<tcp_client>(p->ioc);
    p->client->brokers(host, static_cast<uint16_t>(port));
    p->client->async_run(asio::detached);

    static const std::string payload(64, 'x');

    // `lanes` concurrent self-chaining publish loops per connection -
    // QoS 0's completion handler fires on local write-buffer handoff, not
    // a broker round trip, so a single serial chain undercounts achievable
    // throughput; pipelining several lanes keeps more writes in flight.
    std::vector<std::function<void()>> loops(lanes);
    for (int i = 0; i < lanes; i++) {
        loops[i] = [p, &topic, i, &loops]() {
            if (g_stop_publishing.load(std::memory_order_relaxed)) return;
            p->client->async_publish<mqtt5::qos_e::at_most_once>(
                topic, payload, mqtt5::retain_e::no, mqtt5::publish_props{},
                [i, &loops](boost::system::error_code ec) {
                    if (!ec) g_published.fetch_add(1, std::memory_order_relaxed);
                    if (!g_stop_publishing.load(std::memory_order_relaxed)) loops[i]();
                });
        };
    }
    for (auto& l : loops) l();

    while (!g_stop_publishing.load(std::memory_order_relaxed)) {
        p->ioc.run_one_for(std::chrono::milliseconds(200));
    }
    // Drain any already-in-flight completions briefly before disconnect.
    p->ioc.run_for(std::chrono::milliseconds(300));
    p->client->async_disconnect(asio::detached);
    p->ioc.run_for(std::chrono::milliseconds(200));
}

int main(int argc, char** argv)
{
    if (argc < 7) {
        std::fprintf(stderr,
            "usage: %s <host> <port> <topic> <num_subscribers> "
            "<num_publisher_conns> <duration_sec> [lanes_per_publisher=16]\n",
            argv[0]);
        return 1;
    }
    std::string host = argv[1];
    int port = std::atoi(argv[2]);
    std::string topic = argv[3];
    int n_sub = std::atoi(argv[4]);
    int n_pub = std::atoi(argv[5]);
    int duration_sec = std::atoi(argv[6]);
    int lanes = argc > 7 ? std::atoi(argv[7]) : 16;

    std::string share_topic = "$share/benchgroup/" + topic;

    std::vector<std::unique_ptr<Subscriber>> subs;
    for (int i = 0; i < n_sub; i++) {
        subs.push_back(std::make_unique<Subscriber>());
        Subscriber* s = subs.back().get();
        s->thr = std::thread(subscriber_run, s, host, port, share_topic);
    }
    for (auto& s : subs) {
        while (!s->ready.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::fprintf(stderr, "[bench] %d subscribers ready on %s\n", n_sub, share_topic.c_str());

    std::vector<std::unique_ptr<Publisher>> pubs;
    auto t_start = std::chrono::steady_clock::now();
    for (int i = 0; i < n_pub; i++) {
        pubs.push_back(std::make_unique<Publisher>());
        Publisher* p = pubs.back().get();
        p->thr = std::thread(publisher_run, p, host, port, topic, lanes);
    }

    // Sample every 500ms through the whole run (publish + drain) so the
    // actual receive curve is visible - proves whether consumption kept
    // pace during publishing or whether there's a long undrained-backlog
    // tail after publishing stops (the exact trap flagged in this bench's
    // header comment).
    auto next_sample = t_start + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < t_start + std::chrono::seconds(duration_sec)) {
        std::this_thread::sleep_until(next_sample);
        std::fprintf(stderr, "[bench] t=%.1fs published=%lu received=%lu\n",
                     std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count(),
                     (unsigned long) g_published.load(), (unsigned long) g_received.load());
        next_sample += std::chrono::milliseconds(500);
    }
    g_stop_publishing.store(true);
    for (auto& p : pubs) p->thr.join();
    auto t_pub_stop = std::chrono::steady_clock::now();

    std::fprintf(stderr, "[bench] publishing stopped: published=%lu received=%lu, draining consumers...\n",
                 (unsigned long) g_published.load(), (unsigned long) g_received.load());

    // Drain until received count stops climbing across 3 consecutive
    // 200ms samples, capped at 15s grace period.
    uint64_t last = g_received.load();
    int stable = 0;
    auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (stable < 3 && std::chrono::steady_clock::now() < drain_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        uint64_t now = g_received.load();
        std::fprintf(stderr, "[bench] draining: received=%lu (+%lu)\n",
                     (unsigned long) now, (unsigned long) (now - last));
        if (now == last) stable++;
        else stable = 0;
        last = now;
    }
    auto t_end = std::chrono::steady_clock::now();

    for (auto& s : subs) s->stop.store(true);
    for (auto& s : subs) s->thr.join();

    double wall_pub_s = std::chrono::duration<double>(t_pub_stop - t_start).count();
    double wall_total_s = std::chrono::duration<double>(t_end - t_start).count();
    uint64_t published = g_published.load();
    uint64_t received = g_received.load();

    std::printf("N=%d published=%lu received=%lu wall_publish_s=%.3f wall_total_s=%.3f "
                "publish_rate=%.0f consume_rate_over_publish_window=%.0f "
                "consume_rate_over_total_window=%.0f loss_pct=%.2f\n",
                n_sub, (unsigned long) published, (unsigned long) received,
                wall_pub_s, wall_total_s,
                published / wall_pub_s,
                received / wall_pub_s,
                received / wall_total_s,
                published ? 100.0 * (1.0 - (double) received / (double) published) : 0.0);
    return 0;
}
