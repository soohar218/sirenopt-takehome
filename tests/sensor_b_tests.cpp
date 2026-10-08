#include "sirenopt/sensor_b.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace sirenopt;
constexpr auto kSequenceOrder = BOutputMode::SequenceOrder;
constexpr auto kDefaultTimeout = std::chrono::milliseconds(20);

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::vector<std::uint32_t> sequences(const SampleBatch& samples) {
    std::vector<std::uint32_t> result;
    for (const auto& sample : samples) {
        check(sample.sensor == SensorId::B, "wrong sensor id");
        result.push_back(std::get<NetworkReading>(sample.reading).sequence);
    }
    return result;
}

void test_codec() {
    const BPacket packet{0x01020304u, 0x0102030405060708ull, 1.5};
    const auto bytes = encode_b_packet(packet);
    check(bytes[0] == 'S' && bytes[1] == 'B' && bytes[2] == 1 &&
              bytes[3] == 0, "wire header");
    check(bytes[4] == 1 && bytes[5] == 2 && bytes[6] == 3 &&
              bytes[7] == 4, "sequence byte order");
    check(bytes[8] == 1 && bytes[15] == 8, "timestamp byte order");
    check(bytes[16] == 0x3f && bytes[17] == 0xf8 && bytes[23] == 0,
          "value byte order");
    const auto decoded = decode_b_packet(bytes.data(), bytes.size());
    check(decoded && decoded->sequence == packet.sequence &&
              decoded->device_timestamp_ns == packet.device_timestamp_ns &&
              decoded->value == packet.value, "packet round trip");

    auto bad = bytes;
    check(!decode_b_packet(bad.data(), bad.size() - 1), "short packet");
    check(!decode_b_packet(bad.data(), bad.size() + 1), "long packet");
    check(!decode_b_packet(nullptr, bad.size()), "null packet");
    bad[0] = 0;
    check(!decode_b_packet(bad.data(), bad.size()), "bad magic");
    bad = bytes;
    bad[2] = 2;
    check(!decode_b_packet(bad.data(), bad.size()), "bad version");
    bad = bytes;
    bad[3] = 1;
    check(!decode_b_packet(bad.data(), bad.size()), "bad flags");
    bad = encode_b_packet({1, 2, std::numeric_limits<double>::quiet_NaN()});
    check(!decode_b_packet(bad.data(), bad.size()), "NaN value");
}

void test_reorder() {
    BReorderBuffer buffer(3, kDefaultTimeout, kSequenceOrder);
    const auto now = std::chrono::steady_clock::now();
    auto result = buffer.ingest({10, 100, 10.0}, now);
    check(sequences(result.ready) == std::vector<std::uint32_t>{10},
          "first packet emitted");
    result = buffer.ingest({12, 120, 12.0}, now);
    check(result.ready.empty(), "out-of-order packet buffered");
    result = buffer.ingest({12, 120, 12.0}, now);
    check(result.disposition == PacketDisposition::duplicate,
          "pending duplicate");
    result = buffer.ingest({11, 110, 11.0}, now);
    check(sequences(result.ready) == (std::vector<std::uint32_t>{11, 12}),
          "buffered packet released in order");
    check(result.ready[0].device_timestamp_ns == 110,
          "device timestamp preserved");
    result = buffer.ingest({10, 100, 10.0}, now);
    check(result.disposition == PacketDisposition::duplicate,
          "emitted duplicate");
    result = buffer.ingest({8, 80, 8.0}, now);
    check(result.disposition == PacketDisposition::late, "late packet");
    result = buffer.ingest({15, 150, 15.0}, now);
    check(result.ready.empty(), "future packet buffered");
    result = buffer.ingest({16, 160, 16.0}, now);
    check(result.gaps == 1 && result.ready.empty(),
          "bounded window declares first gap");
    result = buffer.flush();
    check(result.gaps == 1 &&
              sequences(result.ready) ==
                  (std::vector<std::uint32_t>{15, 16}),
          "flush declares remaining gaps");
}

void test_wraparound() {
    BReorderBuffer buffer(4, kDefaultTimeout, kSequenceOrder);
    const auto now = std::chrono::steady_clock::now();
    check(sequences(buffer.ingest({0xfffffffeu, 1, 1.0}, now).ready) ==
              std::vector<std::uint32_t>{0xfffffffeu}, "wrap start");
    check(buffer.ingest({0, 3, 3.0}, now).ready.empty(), "wrap buffer");
    check(sequences(buffer.ingest({0xffffffffu, 2, 2.0}, now).ready) ==
              (std::vector<std::uint32_t>{0xffffffffu, 0}),
          "wrap ordering");
}

void test_large_gap() {
    BReorderBuffer buffer(4, kDefaultTimeout, kSequenceOrder);
    const auto now = std::chrono::steady_clock::now();
    buffer.ingest({1, 1, 1.0}, now);
    const auto result = buffer.ingest({1000000, 2, 2.0}, now);
    check(result.gaps == 999995 && result.ready.empty(),
          "large gap advances without per-packet work");
    const auto remaining = buffer.flush();
    check(remaining.gaps == 3 &&
              sequences(remaining.ready) ==
                  std::vector<std::uint32_t>{1000000},
          "large gap flush");
}

void test_timeout_before_late_arrival() {
    const HostTime start{};
    BReorderBuffer buffer(16, kDefaultTimeout, kSequenceOrder);
    check(sequences(buffer.ingest({1, 100, 1.5}, start).ready) ==
              std::vector<std::uint32_t>{1}, "timeout baseline");
    check(buffer.ingest({3, 300, 3.5}, start +
                            std::chrono::milliseconds(1)).ready.empty(),
          "future packet waits");
    check(buffer.expire(start + std::chrono::milliseconds(20)).ready.empty(),
          "timeout has not elapsed");
    auto result = buffer.expire(start + std::chrono::milliseconds(21));
    check(result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{3},
          "idle timeout finalizes gap and releases packet");
    check(result.ready[0].host_timestamp ==
              start + std::chrono::milliseconds(1),
          "original arrival timestamp preserved");
    result = buffer.ingest({2, 200, 2.5},
                           start + std::chrono::milliseconds(22));
    check(result.disposition == PacketDisposition::late &&
              result.gaps == 0 && result.ready.empty(),
          "packet after finalized gap is late");
    check(buffer.flush().gaps == 0, "shutdown does not repeat timeout gap");
}

void test_reorder_before_timeout() {
    const HostTime start{};
    BReorderBuffer buffer(16, kDefaultTimeout, kSequenceOrder);
    buffer.ingest({1, 100, 1.5}, start);
    buffer.ingest({3, 300, 3.5}, start + std::chrono::milliseconds(1));
    auto result = buffer.ingest({2, 200, 2.5},
                                start + std::chrono::milliseconds(19));
    check(result.gaps == 0 &&
              sequences(result.ready) ==
                  (std::vector<std::uint32_t>{2, 3}),
          "packet before timeout reorders without gap");
    check(buffer.expire(start + std::chrono::milliseconds(100)).gaps == 0,
          "resolved gap stays resolved");
}

void test_late_arrival_triggers_expiry() {
    const HostTime start{};
    BReorderBuffer buffer(16, kDefaultTimeout, kSequenceOrder);
    buffer.ingest({1, 100, 1.5}, start);
    buffer.ingest({3, 300, 3.5}, start + std::chrono::milliseconds(1));
    auto result = buffer.ingest({2, 200, 2.5},
                                start + std::chrono::milliseconds(22));
    check(result.disposition == PacketDisposition::late &&
              result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{3},
          "late arrival itself triggers expiry first");
}

void test_window_before_timeout() {
    const HostTime start{};
    BReorderBuffer buffer(2, kDefaultTimeout, kSequenceOrder);
    buffer.ingest({1, 100, 1.5}, start);
    buffer.ingest({3, 300, 3.5}, start + std::chrono::milliseconds(1));
    auto result = buffer.ingest({4, 400, 4.5},
                                start + std::chrono::milliseconds(2));
    check(result.gaps == 1 &&
              sequences(result.ready) ==
                  (std::vector<std::uint32_t>{3, 4}),
          "window finalizes gap before timeout");
}

void test_multiple_missing_and_short_timeout() {
    const HostTime start{};
    BReorderBuffer buffer(16, std::chrono::milliseconds(5), kSequenceOrder);
    buffer.ingest({1, 100, 1.5}, start);
    buffer.ingest({4, 400, 4.5}, start + std::chrono::milliseconds(1));
    auto result = buffer.expire(start + std::chrono::milliseconds(6));
    check(result.gaps == 2 &&
              sequences(result.ready) == std::vector<std::uint32_t>{4},
          "short timeout finalizes consecutive gaps");
    check(buffer.flush().gaps == 0, "no duplicate gaps at shutdown");

    BReorderBuffer staggered(16, std::chrono::milliseconds(5), kSequenceOrder);
    staggered.ingest({1, 100, 1.5}, start);
    staggered.ingest({3, 300, 3.5}, start + std::chrono::milliseconds(1));
    staggered.ingest({5, 500, 5.5}, start + std::chrono::milliseconds(4));
    result = staggered.expire(start + std::chrono::milliseconds(6));
    check(result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{3},
          "later hole keeps its own detection time");
    result = staggered.expire(start + std::chrono::milliseconds(9));
    check(result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{5},
          "later hole expires at its own deadline");

    BReorderBuffer shutdown(16, std::chrono::milliseconds(5), kSequenceOrder);
    shutdown.ingest({1, 100, 1.5}, start);
    shutdown.ingest({3, 300, 3.5}, start + std::chrono::milliseconds(1));
    result = shutdown.flush();
    check(result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{3},
          "shutdown flushes pending gap once");
    check(shutdown.expire(start + std::chrono::milliseconds(100)).gaps == 0,
          "post-shutdown expiry does not double count");
}

void test_arrival_order_tracking() {
    const SensorBConfig defaults;
    check(defaults.output_mode == BOutputMode::ArrivalOrder &&
              defaults.reorder_window == 16 &&
              defaults.reorder_timeout == kDefaultTimeout,
          "arrival mode and existing tracking defaults");

    const HostTime start{};
    BReorderBuffer buffer(16);
    auto result = buffer.ingest({10, 100, 10.5}, start);
    check(sequences(result.ready) == std::vector<std::uint32_t>{10},
          "arrival mode emits first packet");
    result = buffer.ingest({12, 120, 12.5},
                           start + std::chrono::milliseconds(1));
    check(sequences(result.ready) == std::vector<std::uint32_t>{12} &&
              result.out_of_order == 1 && result.gaps == 0,
          "future packet forwarded immediately");
    check(result.ready[0].host_timestamp ==
                  start + std::chrono::milliseconds(1) &&
              result.ready[0].device_timestamp_ns == 120 &&
              std::get<NetworkReading>(result.ready[0].reading).value == 12.5,
          "arrival and device metadata preserved");
    result = buffer.ingest({11, 110, 11.5},
                           start + std::chrono::milliseconds(2));
    check(sequences(result.ready) == std::vector<std::uint32_t>{11} &&
              result.out_of_order == 1 && result.recovered == 1 &&
              result.gaps == 0,
          "missing sequence recovered without delayed output");
    result = buffer.ingest({11, 110, 11.5},
                           start + std::chrono::milliseconds(3));
    check(result.disposition == PacketDisposition::duplicate &&
              result.ready.empty(), "arrival mode duplicate rejected");
    result = buffer.ingest({14, 140, 14.5},
                           start + std::chrono::milliseconds(4));
    check(sequences(result.ready) == std::vector<std::uint32_t>{14},
          "later packet forwarded during gap");
    result = buffer.expire(start + std::chrono::milliseconds(24));
    check(result.gaps == 1 && result.ready.empty(),
          "unrecovered gap finalized without duplicate output");
    result = buffer.ingest({13, 130, 13.5},
                           start + std::chrono::milliseconds(25));
    check(result.disposition == PacketDisposition::late &&
              result.ready.empty(), "finalized gap arrives late");
    check(buffer.flush().gaps == 0, "arrival mode flush does not recount");
}

void test_arrival_order_multiple_recoveries_and_window() {
    const HostTime start{};
    BReorderBuffer buffer(16);
    buffer.ingest({1, 1, 1.0}, start);
    auto result = buffer.ingest({4, 4, 4.0},
                                start + std::chrono::milliseconds(1));
    check(sequences(result.ready) == std::vector<std::uint32_t>{4},
          "arrival mode forwards across multiple holes");
    result = buffer.ingest({2, 2, 2.0},
                           start + std::chrono::milliseconds(2));
    check(result.recovered == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{2},
          "first missing sequence recovered");
    result = buffer.ingest({3, 3, 3.0},
                           start + std::chrono::milliseconds(3));
    check(result.recovered == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{3} &&
              buffer.flush().gaps == 0,
          "second missing sequence recovered");

    BReorderBuffer window(2);
    window.ingest({1, 1, 1.0}, start);
    window.ingest({3, 3, 3.0}, start + std::chrono::milliseconds(1));
    result = window.ingest({4, 4, 4.0},
                           start + std::chrono::milliseconds(2));
    check(result.gaps == 1 &&
              sequences(result.ready) == std::vector<std::uint32_t>{4} &&
              window.flush().gaps == 0,
          "window gap does not repeat previously emitted packets");
}

class CollectSink final : public ISampleSink {
public:
    bool try_submit(SampleBatch&& batch) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.insert(samples_.end(),
                        std::make_move_iterator(batch.begin()),
                        std::make_move_iterator(batch.end()));
        return true;
    }

    SampleBatch snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_;
    }

private:
    mutable std::mutex mutex_;
    SampleBatch samples_;
};

void wait_until(const std::function<bool()>& condition, const char* message) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(2);
    while (!condition()) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error(message);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

void send_packet(std::uint16_t port, const BPacket& packet) {
    const int socket_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    check(socket_fd >= 0, "late packet test socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const auto bytes = encode_b_packet(packet);
    const auto sent = ::sendto(socket_fd, bytes.data(), bytes.size(), 0,
                               reinterpret_cast<const sockaddr*>(&address),
                               sizeof(address));
    ::close(socket_fd);
    check(sent == static_cast<ssize_t>(bytes.size()),
          "late packet test send");
}

void test_udp(bool inject_faults) {
    SensorB sensor({0, 4, kDefaultTimeout, kSequenceOrder});
    SensorStats stats;
    CollectSink sink;
    std::atomic<bool> stop{false};
    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            sensor.run(sink, stop, stats);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    try {
        wait_until([&] { return sensor.bound_port() != 0; },
                   "receiver did not bind");
        MockBSenderConfig config;
        config.port = sensor.bound_port();
        config.packet_limit = inject_faults ? 20 : 8;
        config.interval_us = 1000;
        config.loss_every = inject_faults ? 5 : 0;
        config.reorder_every = inject_faults ? 4 : 0;
        config.duplicate_every = inject_faults ? 7 : 0;
        config.malformed_every = inject_faults ? 9 : 0;
        std::exception_ptr sender_error;
        std::thread sender([&] {
            try {
                MockBSender(config).run(stop);
            } catch (...) {
                sender_error = std::current_exception();
            }
        });
        sender.join();
        if (sender_error) std::rethrow_exception(sender_error);
        const std::size_t expected = inject_faults ? 14 : 8;
        wait_until([&] { return stats.received.load() == expected; },
                   "receiver did not process expected packets");
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
        const auto samples = sink.snapshot();
        check(samples.size() == expected, "UDP output count");
        const auto seq = sequences(samples);
        check(std::is_sorted(seq.begin(), seq.end()), "UDP sequence order");
        for (const auto& sample : samples) {
            check(sample.device_timestamp_ns.has_value(),
                  "UDP device timestamp missing");
            check(sample.host_timestamp <= std::chrono::steady_clock::now(),
                  "UDP host arrival timestamp missing");
        }
        const auto counts = stats.snapshot();
        if (inject_faults) {
            check(counts.received == 14 && counts.dropped == 0 &&
                      counts.rejected == 4 && counts.gaps == 5 &&
                      counts.duplicates == 2 && counts.late == 0,
                  "fault injection counters");
            const auto packet3 = std::find_if(samples.begin(), samples.end(),
                                              [](const Sample& sample) {
                return std::get<NetworkReading>(sample.reading).sequence == 3;
            });
            const auto packet5 = std::find_if(samples.begin(), samples.end(),
                                              [](const Sample& sample) {
                return std::get<NetworkReading>(sample.reading).sequence == 5;
            });
            check(packet3 != samples.end() && packet5 != samples.end() &&
                      packet5->host_timestamp < packet3->host_timestamp,
                  "reordered UDP arrival preserved");
            std::cout << "UDP output sequence/value: ";
            for (std::size_t i = 0; i < std::min<std::size_t>(8, samples.size());
                 ++i) {
                const auto reading =
                    std::get<NetworkReading>(samples[i].reading);
                if (i) std::cout << ", ";
                std::cout << reading.sequence << '/' << reading.value;
            }
            std::cout << '\n'
                      << "UDP arrival order: seq 5 before seq 3; emitted "
                         "seq 3 before seq 5\n";
            std::cout << "UDP faults: received=" << counts.received
                      << " dropped=" << counts.dropped
                      << " rejected=" << counts.rejected
                      << " gaps=" << counts.gaps
                      << " duplicates=" << counts.duplicates
                      << " late=" << counts.late << '\n';
        } else {
            check(counts.received == 8 && counts.dropped == 0 &&
                      counts.rejected == 0 && counts.gaps == 0,
                  "clean UDP counters");
            std::cout << "UDP clean: received=" << counts.received
                      << " rejected=" << counts.rejected << '\n';
        }
    } catch (...) {
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
        throw;
    }
}

void test_udp_idle_timeout() {
    SensorB sensor({0, 16, kDefaultTimeout, kSequenceOrder});
    SensorStats stats;
    CollectSink sink;
    std::atomic<bool> stop{false};
    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            sensor.run(sink, stop, stats);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });
    try {
        wait_until([&] { return sensor.bound_port() != 0; },
                   "idle timeout receiver did not bind");
        MockBSenderConfig config;
        config.port = sensor.bound_port();
        config.packet_limit = 3;
        config.loss_every = 2;
        config.reorder_every = 0;
        config.duplicate_every = 0;
        config.malformed_every = 0;
        MockBSender(config).run(stop);
        wait_until([&] { return sink.snapshot().size() == 2; },
                   "idle UDP timeout did not release buffered packet");
        check(!stop.load(), "idle release required receiver to keep running");
        const auto output = sink.snapshot();
        check(sequences(output) == (std::vector<std::uint32_t>{0, 2}) &&
                  stats.gaps.load() == 1,
              "idle UDP timeout output and gap count");
        std::cout << "UDP idle timeout: output 0,2; gap 1 finalized "
                     "without another datagram\n";
        send_packet(sensor.bound_port(), {1, 1000000, 1.5});
        wait_until([&] { return stats.late.load() == 1; },
                   "late UDP packet was not rejected");
        check(stats.rejected.load() == 1 &&
                  sink.snapshot().size() == 2,
              "late UDP packet counters and output");
        std::cout << "UDP late after timeout: late=1 rejected=1\n";
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
        check(stats.gaps.load() == 1,
              "idle UDP shutdown did not count gap twice");
    } catch (...) {
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
        throw;
    }
}

void test_udp_arrival_order() {
    SensorB sensor({0, 16, std::chrono::milliseconds(100),
                    BOutputMode::ArrivalOrder});
    SensorStats stats;
    CollectSink sink;
    std::atomic<bool> stop{false};
    std::exception_ptr receiver_error;
    std::thread receiver([&] {
        try {
            sensor.run(sink, stop, stats);
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });
    try {
        wait_until([&] { return sensor.bound_port() != 0; },
                   "arrival receiver did not bind");
        const auto port = sensor.bound_port();
        send_packet(port, {10, 100, 10.5});
        wait_until([&] { return sink.snapshot().size() == 1; },
                   "sequence 10 was not forwarded");
        send_packet(port, {12, 120, 12.5});
        wait_until([&] { return sink.snapshot().size() == 2; },
                   "sequence 12 was delayed");
        send_packet(port, {11, 110, 11.5});
        wait_until([&] { return sink.snapshot().size() == 3; },
                   "sequence 11 was not forwarded");
        send_packet(port, {11, 110, 11.5});
        wait_until([&] { return stats.duplicates.load() == 1; },
                   "arrival duplicate was not rejected");
        send_packet(port, {14, 140, 14.5});
        wait_until([&] { return sink.snapshot().size() == 4; },
                   "sequence 14 was delayed");
        wait_until([&] { return stats.gaps.load() == 1; },
                   "arrival gap did not expire while idle");
        const auto output = sink.snapshot();
        check(sequences(output) ==
                  (std::vector<std::uint32_t>{10, 12, 11, 14}),
              "UDP output follows host arrival order");
        check(output[1].host_timestamp < output[2].host_timestamp &&
                  output[1].device_timestamp_ns == 120 &&
                  std::get<NetworkReading>(output[2].reading).value == 11.5,
              "UDP arrival timestamps and metadata retained");
        const auto counts = stats.snapshot();
        check(counts.received == 4 && counts.rejected == 1 &&
                  counts.duplicates == 1 && counts.gaps == 1 &&
                  counts.recovered == 1 && counts.out_of_order == 3,
              "arrival tracking counters");
        std::cout << "UDP arrival mode: output 10,12,11,14; recovered="
                  << counts.recovered << " gaps=" << counts.gaps
                  << " duplicates=" << counts.duplicates << '\n';
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
    } catch (...) {
        stop.store(true);
        receiver.join();
        if (receiver_error) std::rethrow_exception(receiver_error);
        throw;
    }
}

}  // namespace

int main() {
    try {
        test_codec();
        test_reorder();
        test_wraparound();
        test_large_gap();
        test_timeout_before_late_arrival();
        test_reorder_before_timeout();
        test_late_arrival_triggers_expiry();
        test_window_before_timeout();
        test_multiple_missing_and_short_timeout();
        test_arrival_order_tracking();
        test_arrival_order_multiple_recoveries_and_window();
        test_udp(false);
        test_udp(true);
        test_udp_idle_timeout();
        test_udp_arrival_order();
        std::cout << "Sensor B tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Sensor B test failed: " << error.what() << '\n';
        return 1;
    }
}
