#include "sirenopt/sensor_b.hpp"

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
    BReorderBuffer buffer(3);
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
    BReorderBuffer buffer(4);
    const auto now = std::chrono::steady_clock::now();
    check(sequences(buffer.ingest({0xfffffffeu, 1, 1.0}, now).ready) ==
              std::vector<std::uint32_t>{0xfffffffeu}, "wrap start");
    check(buffer.ingest({0, 3, 3.0}, now).ready.empty(), "wrap buffer");
    check(sequences(buffer.ingest({0xffffffffu, 2, 2.0}, now).ready) ==
              (std::vector<std::uint32_t>{0xffffffffu, 0}),
          "wrap ordering");
}

void test_large_gap() {
    BReorderBuffer buffer(4);
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

void test_udp(bool inject_faults) {
    SensorB sensor({0, 4});
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

}  // namespace

int main() {
    try {
        test_codec();
        test_reorder();
        test_wraparound();
        test_large_gap();
        test_udp(false);
        test_udp(true);
        std::cout << "Sensor B tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Sensor B test failed: " << error.what() << '\n';
        return 1;
    }
}
