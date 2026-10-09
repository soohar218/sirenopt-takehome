#include "sirenopt/merge.hpp"
#include "sirenopt/mock_sensors.hpp"
#include "sirenopt/sensor_b.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace {

using namespace sirenopt;
using namespace std::chrono_literals;

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class CollectConsumer final : public IMergedConsumer {
public:
    void consume(Sample&& sample) override {
        samples.push_back(std::move(sample));
    }

    std::vector<Sample> samples;
};

class ConcurrentConsumer final : public IMergedConsumer {
public:
    void consume(Sample&& sample) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            samples_.push_back(std::move(sample));
        }
        available_.notify_one();
    }

    bool wait_for_count(std::size_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return available_.wait_for(lock, 2s,
                                   [&] { return samples_.size() >= count; });
    }

    std::vector<Sample> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable available_;
    std::vector<Sample> samples_;
};

class CollectSink final : public ISampleSink {
public:
    bool try_submit(SampleBatch&& batch) noexcept override {
        batches.push_back(std::move(batch));
        return true;
    }

    std::vector<SampleBatch> batches;
};

Sample make_sample(SensorId id, HostTime timestamp, std::uint64_t tag) {
    if (id == SensorId::A) {
        return {id, timestamp, tag, AnalogReading{static_cast<double>(tag)}};
    }
    if (id == SensorId::B) {
        return {id, timestamp, tag,
                NetworkReading{static_cast<double>(tag),
                               static_cast<std::uint32_t>(tag)}};
    }
    return {id, timestamp, tag,
            SerialReading{static_cast<double>(tag), 1013.2}};
}

void test_default_holdback_boundary() {
    SensorStats stats;
    MergeBuffer merge;
    CollectConsumer consumer;
    const auto observed = HostTime{} + 10ms;
    merge.push(make_sample(SensorId::B, observed, 1), stats);
    merge.emit_ready(observed + 9ms, consumer);
    check(consumer.samples.empty(), "default holdback retains sample at 9 ms");
    merge.emit_ready(observed + 10ms, consumer);
    check(consumer.samples.size() == 1 &&
              consumer.samples[0].host_timestamp == observed &&
              merge.stats().emitted == 1,
          "default holdback releases sample at 10 ms");
}

void test_late_analog_callback_drops_whole_batch() {
    SensorStats a_stats, b_stats;
    MergeBuffer merge({5ms, 1200});
    CollectConsumer consumer;
    const auto observed = HostTime{} + 10ms;
    merge.push(make_sample(SensorId::B, observed, 1), b_stats);
    merge.emit_ready(observed + 5ms, consumer);

    auto batch = make_analog_block(0, observed - 1ms);
    check(batch.size() == 1000 &&
              std::all_of(batch.begin(), batch.end(), [&](const Sample& sample) {
                  return sample.host_timestamp == batch.front().host_timestamp;
              }),
          "A callback gives all 1000 samples one host timestamp");
    a_stats.received.store(batch.size());
    for (auto& sample : batch) merge.push(std::move(sample), a_stats);
    merge.emit_all(consumer);
    check(merge.stats().late_dropped == 1000 &&
              a_stats.dropped.load() == 1000 &&
              consumer.samples.size() == 1 &&
              consumer.samples.front().sensor == SensorId::B &&
              merge.size_samples() == 0,
          "one late callback accounts for all 1000 A samples");
}

void test_ordering_and_ties() {
    const HostTime start{};
    SensorStats a_stats, b_stats, c_stats;
    MergeBuffer merge({30ms, 16});
    CollectConsumer consumer;
    merge.push(make_sample(SensorId::B, start + 20ms, 1), b_stats);
    merge.push(make_sample(SensorId::C, start + 15ms, 2), c_stats);
    merge.push(make_sample(SensorId::A, start + 10ms, 3), a_stats);
    merge.emit_ready(start + 39ms, consumer);
    check(consumer.samples.empty(), "holdback excludes just-before-boundary");
    merge.emit_ready(start + 40ms, consumer);
    check(consumer.samples.size() == 1 &&
              consumer.samples[0].sensor == SensorId::A,
          "holdback includes exact boundary");
    merge.emit_ready(start + 50ms, consumer);
    check(consumer.samples.size() == 3 &&
              consumer.samples[1].sensor == SensorId::C &&
              consumer.samples[2].sensor == SensorId::B,
          "global host timestamp ordering");

    const auto tied = start + 60ms;
    merge.push(make_sample(SensorId::C, tied, 4), c_stats);
    merge.push(make_sample(SensorId::B, tied, 5), b_stats);
    merge.push(make_sample(SensorId::A, tied, 6), a_stats);
    merge.push(make_sample(SensorId::A, tied, 7), a_stats);
    merge.emit_all(consumer);
    std::vector<std::tuple<HostTime, SensorId, std::uint64_t>> actual;
    for (const auto& sample : consumer.samples) {
        actual.emplace_back(sample.host_timestamp, sample.sensor,
                            *sample.device_timestamp_ns);
    }
    const std::vector<std::tuple<HostTime, SensorId, std::uint64_t>> expected{
        {start + 10ms, SensorId::A, 3},
        {start + 15ms, SensorId::C, 2},
        {start + 20ms, SensorId::B, 1},
        {tied, SensorId::A, 6},
        {tied, SensorId::A, 7},
        {tied, SensorId::B, 5},
        {tied, SensorId::C, 4},
    };
    check(actual == expected && merge.stats().emitted == expected.size(),
          "exact timestamp, sensor tie, and original sample order");
}

void test_equal_timestamp_after_emission_is_not_late() {
    const HostTime start{};
    const auto tied = start + 50ms;
    SensorStats a_stats, b_stats, c_stats;
    a_stats.received.store(2);
    b_stats.received.store(3);
    c_stats.received.store(1);
    MergeBuffer merge({30ms, 8});
    CollectConsumer consumer;

    merge.push(make_sample(SensorId::B, tied, 1), b_stats);
    merge.emit_ready(tied + 30ms, consumer);
    check(consumer.samples.size() == 1, "first tied sample emitted");

    merge.push(make_sample(SensorId::C, tied, 2), c_stats);
    merge.push(make_sample(SensorId::A, tied, 3), a_stats);
    merge.push(make_sample(SensorId::B, tied - 1ms, 4), b_stats);
    merge.push(make_sample(SensorId::A, tied, 5), a_stats);
    merge.push(make_sample(SensorId::B, tied + 1ms, 6), b_stats);
    merge.emit_all(consumer);

    std::vector<std::tuple<HostTime, SensorId, std::uint64_t>> actual;
    for (const auto& sample : consumer.samples) {
        actual.emplace_back(sample.host_timestamp, sample.sensor,
                            *sample.device_timestamp_ns);
    }
    const std::vector<std::tuple<HostTime, SensorId, std::uint64_t>> expected{
        {tied, SensorId::B, 1},
        {tied, SensorId::A, 3},
        {tied, SensorId::A, 5},
        {tied, SensorId::C, 2},
        {tied + 1ms, SensorId::B, 6},
    };
    const auto counts = merge.stats();
    check(actual == expected && counts.emitted == 5 &&
              counts.late_dropped == 1 && counts.overflow_dropped == 0 &&
              a_stats.dropped.load() == 0 &&
              b_stats.dropped.load() == 1 &&
              c_stats.dropped.load() == 0 &&
              a_stats.received.load() + b_stats.received.load() +
                      c_stats.received.load() ==
                  counts.emitted + a_stats.dropped.load() +
                      b_stats.dropped.load() + c_stats.dropped.load(),
          "equal-time ties accepted and sorted; only older timestamp dropped");
}

void test_late_and_bounded_capacity() {
    const HostTime start{};
    SensorStats a_stats, b_stats, c_stats;
    a_stats.received.store(3);
    b_stats.received.store(2);
    c_stats.received.store(2);
    MergeBuffer merge({30ms, 2});
    CollectConsumer consumer;
    merge.push(make_sample(SensorId::B, start + 10ms, 1), b_stats);
    merge.emit_ready(start + 40ms, consumer);
    merge.push(make_sample(SensorId::A, start + 9ms, 2), a_stats);
    merge.push(make_sample(SensorId::A, start + 10ms, 3), a_stats);
    merge.push(make_sample(SensorId::C, start + 10ms, 4), c_stats);
    merge.emit_ready(start + 40ms, consumer);
    check(merge.stats().late_dropped == 1 &&
              a_stats.dropped.load() == 1 &&
              consumer.samples.size() == 3 &&
              consumer.samples[0].sensor == SensorId::B &&
              consumer.samples[1].sensor == SensorId::A &&
              consumer.samples[2].sensor == SensorId::C,
          "only older timestamps are late; equal-time A is retained");

    merge.push(make_sample(SensorId::A, start + 50ms, 5), a_stats);
    merge.push(make_sample(SensorId::B, start + 51ms, 6), b_stats);
    merge.push(make_sample(SensorId::C, start + 52ms, 7), c_stats);
    check(merge.size_samples() == 2 &&
              merge.stats().overflow_dropped == 1 &&
              c_stats.dropped.load() == 1,
          "merge capacity is bounded in samples");
    merge.emit_all(consumer);
    std::vector<std::pair<SensorId, std::uint64_t>> actual;
    for (const auto& sample : consumer.samples) {
        actual.emplace_back(sample.sensor, *sample.device_timestamp_ns);
    }
    check(actual == (std::vector<std::pair<SensorId, std::uint64_t>>{
                        {SensorId::B, 1}, {SensorId::A, 3},
                        {SensorId::C, 4},
                        {SensorId::A, 5}, {SensorId::B, 6}}) &&
              merge.size_samples() == 0 && merge.stats().emitted == 5 &&
              merge.stats().late_dropped == 1 &&
              merge.stats().overflow_dropped == 1 &&
              a_stats.dropped.load() == 1 &&
              b_stats.dropped.load() == 0 &&
              c_stats.dropped.load() == 1 &&
              a_stats.received.load() == 2 + a_stats.dropped.load() &&
              b_stats.received.load() == 2 + b_stats.dropped.load() &&
              c_stats.received.load() == 1 + c_stats.dropped.load(),
          "exact late and merge-overflow accounting after flush");
}

void test_analog_and_serial_metadata() {
    const auto arrival = HostTime{} + 42ms;
    const auto block = make_analog_block(2, arrival);
    check(block.size() == 1000, "analog block has 1000 samples");
    check(std::all_of(block.begin(), block.end(),
                      [&](const Sample& sample) {
                          return sample.sensor == SensorId::A &&
                                 sample.host_timestamp == arrival;
                      }),
          "all analog samples use callback arrival");
    check(block.front().device_timestamp_ns == 20000000 &&
              block.back().device_timestamp_ns == 29990000,
          "analog measurement times retain 10 us spacing");
    const auto on_time = make_analog_block(0, HostTime{} + 10ms);
    const auto delayed = make_analog_block(1, HostTime{} + 35ms);
    check(delayed.front().host_timestamp - on_time.front().host_timestamp ==
                  25ms &&
              on_time.front().device_timestamp_ns == 0 &&
              on_time.back().device_timestamp_ns == 9990000 &&
              delayed.front().device_timestamp_ns == 10000000 &&
              delayed.back().device_timestamp_ns == 19990000 &&
              std::all_of(delayed.begin(), delayed.end(),
                          [&](const Sample& sample) {
                              return sample.host_timestamp ==
                                     delayed.front().host_timestamp;
                          }),
          "delayed callback changes observation time, not acquisition ticks");

    MockAnalogSensor analog({2, 25ms, 2});
    SensorStats analog_stats;
    CollectSink analog_sink;
    std::atomic<bool> analog_stop{false};
    analog.run(analog_sink, analog_stop, analog_stats);
    check(analog_sink.batches.size() == 2 &&
              analog_sink.batches[0].size() == 1000 &&
              analog_sink.batches[1].size() == 1000 &&
              analog_sink.batches[1][0].host_timestamp -
                      analog_sink.batches[0][0].host_timestamp >= 25ms &&
              analog_sink.batches[0].back().device_timestamp_ns == 9990000 &&
              analog_sink.batches[1].front().device_timestamp_ns == 10000000 &&
              analog_stats.received.load() == 2000 &&
              analog_stats.dropped.load() == 0,
          "configured late callback preserves acquisition metadata");

    const auto valid = parse_serial_line("T=23.41,P=1013.2");
    check(valid && valid->temperature == 23.41 &&
              valid->pressure == 1013.2,
          "serial line preserves both values");
    check(parse_serial_line("T=23.41,P=1013.2\n") &&
              parse_serial_line("T=23.41,P=1013.2\r\n"),
          "serial line terminators accepted");
    check(!parse_serial_line("T=broken,P=1013.2") &&
              !parse_serial_line("T=23.41,P=") &&
              !parse_serial_line("T=23.41,P=1013.2junk") &&
              !parse_serial_line("T=nan,P=1013.2") &&
              !parse_serial_line("T=23.41,P=1013.2\nT=24,P=1000") &&
              !parse_serial_line("P=1013.2,T=23.41"),
          "corrupt and malformed serial lines rejected");

    MockSerialSensor serial({2, 3});
    SensorStats stats;
    CollectSink sink;
    std::atomic<bool> stop{false};
    serial.run(sink, stop, stats);
    check(stats.received.load() == 2 && stats.rejected.load() == 1 &&
              sink.batches.size() == 2 &&
              sink.batches[0].size() == 1 && sink.batches[1].size() == 1 &&
              sink.batches[0][0].host_timestamp <=
                  sink.batches[1][0].host_timestamp &&
              sink.batches[0][0].device_timestamp_ns == 0 &&
              sink.batches[1][0].device_timestamp_ns == 200000000 &&
              std::get<SerialReading>(sink.batches[0][0].reading).temperature ==
                  23.41 &&
              std::get<SerialReading>(sink.batches[1][0].reading).temperature ==
                  23.61 &&
              std::get<SerialReading>(sink.batches[1][0].reading).pressure ==
                  1013.2,
          "malformed C line rejected and next valid line recovered");
}

void test_b_arrival_order_through_merge() {
    const HostTime start{};
    BReorderBuffer tracker(16, 20ms, BOutputMode::ArrivalOrder);
    SensorStats stats;
    MergeBuffer merge({30ms, 16});
    CollectConsumer consumer;
    const auto feed = [&](const BPacket& packet, HostTime arrival) {
        auto result = tracker.ingest(packet, arrival);
        check(result.disposition == PacketDisposition::accepted &&
                  result.ready.size() == 1,
              "arrival tracker forwards each valid packet immediately");
        stats.received.fetch_add(1, std::memory_order_relaxed);
        merge.push(std::move(result.ready[0]), stats);
    };
    feed({10, 100, 10.5}, start + 10ms);
    feed({12, 120, 12.5}, start + 12ms);
    feed({11, 110, 11.5}, start + 13ms);
    merge.emit_ready(start + 42ms, consumer);
    check(consumer.samples.size() == 2,
          "B third arrival remains held before exact boundary");
    merge.emit_ready(start + 43ms, consumer);
    std::vector<std::tuple<std::uint32_t, HostTime, std::uint64_t>> actual;
    for (const auto& sample : consumer.samples) {
        const auto reading = std::get<NetworkReading>(sample.reading);
        actual.emplace_back(reading.sequence, sample.host_timestamp,
                            *sample.device_timestamp_ns);
    }
    check(actual ==
              (std::vector<std::tuple<std::uint32_t, HostTime,
                                      std::uint64_t>>{
                  {10, start + 10ms, 100},
                  {12, start + 12ms, 120},
                  {11, start + 13ms, 110}}) &&
              stats.received.load() == 3 && stats.dropped.load() == 0 &&
              merge.stats().emitted == 3 &&
              merge.stats().late_dropped == 0,
          "B 10,12,11 keeps host arrival order and device metadata");
}

void test_queue_overflow_accounts_analog_block() {
    SensorStats stats;
    BoundedBatchQueue queue(1, stats);
    stats.received.fetch_add(2000, std::memory_order_relaxed);
    check(queue.try_submit(make_analog_block(0, HostTime{} + 10ms)),
          "first A block enters one-batch queue");
    check(!queue.try_submit(make_analog_block(1, HostTime{} + 20ms)) &&
              stats.dropped.load() == 1000 &&
              queue.size_batches() == 1,
          "full queue drops newest 1000-sample block without waiting");
    auto accepted = queue.try_pop();
    check(accepted && accepted->size() == 1000 &&
              accepted->front().device_timestamp_ns == 0,
          "oldest A block retained");
    MergeBuffer merge({30ms, 1200});
    CollectConsumer consumer;
    for (auto& sample : *accepted) merge.push(std::move(sample), stats);
    merge.emit_all(consumer);
    check(merge.stats().emitted == 1000 &&
              merge.stats().late_dropped == 0 &&
              merge.stats().overflow_dropped == 0 &&
              consumer.samples.size() == 1000 &&
              stats.received.load() == merge.stats().emitted +
                                           stats.dropped.load(),
          "every accepted A sample emitted or explicitly dropped");
}

class IdleSensor final : public ISensor {
public:
    explicit IdleSensor(SensorId id) : id_(id) {}
    SensorId id() const noexcept override { return id_; }
    void run(ISampleSink&, const std::atomic<bool>&, SensorStats&) override {}

private:
    SensorId id_;
};

class ThrowingConsumer final : public IMergedConsumer {
public:
    void consume(Sample&&) override {
        ++calls;
        throw std::runtime_error("consumer failed after receiving sample");
    }
    int calls{0};
};

void test_concurrent_stats_snapshot() {
    SensorAcquisition a(std::make_unique<IdleSensor>(SensorId::A), 2);
    SensorAcquisition b(std::make_unique<IdleSensor>(SensorId::B), 128);
    SensorAcquisition c(std::make_unique<IdleSensor>(SensorId::C), 2);
    ConcurrentConsumer consumer;
    MergeStage merge({&a, &b, &c}, consumer, {0ms, 128});
    merge.start();
    std::atomic<bool> reading{true};
    std::atomic<bool> valid{true};
    std::thread reader([&] {
        std::uint64_t previous = 0;
        while (reading.load(std::memory_order_relaxed)) {
            const auto snapshot = merge.stats();
            if (snapshot.emitted < previous || snapshot.emitted > 100 ||
                snapshot.failed_deliveries != 0) {
                valid.store(false, std::memory_order_relaxed);
            }
            previous = snapshot.emitted;
            std::this_thread::yield();
        }
    });
    const auto timestamp = std::chrono::steady_clock::now() - 1s;
    for (std::uint64_t i = 0; i < 100; ++i) {
        b.stats().received.fetch_add(1, std::memory_order_relaxed);
        SampleBatch batch;
        batch.push_back(make_sample(SensorId::B, timestamp, i));
        check(b.queue().try_submit(std::move(batch)), "stats test queue capacity");
    }
    a.queue().close();
    b.queue().close();
    c.queue().close();
    merge.join();
    reading.store(false, std::memory_order_relaxed);
    reader.join();
    check(valid.load() && merge.stats().emitted == 100 &&
              consumer.snapshot().size() == 100 &&
              b.stats().dropped.load() == 0,
          "concurrent merge stats snapshots and complete drain");
}

void test_consumer_exception() {
    SensorAcquisition a(std::make_unique<IdleSensor>(SensorId::A), 2);
    SensorAcquisition b(std::make_unique<IdleSensor>(SensorId::B), 2);
    SensorAcquisition c(std::make_unique<IdleSensor>(SensorId::C), 2);
    ThrowingConsumer consumer;
    MergeStage merge({&a, &b, &c}, consumer, {0ms, 4});
    const auto timestamp = std::chrono::steady_clock::now() - 1s;
    b.stats().received.fetch_add(1, std::memory_order_relaxed);
    SampleBatch batch;
    batch.push_back(make_sample(SensorId::B, timestamp, 1));
    check(b.queue().try_submit(std::move(batch)), "failed delivery sample queued");
    a.queue().close();
    b.queue().close();
    c.queue().close();
    merge.start();
    bool propagated = false;
    try {
        merge.join();
    } catch (const std::runtime_error&) {
        propagated = true;
    }
    const auto counts = merge.stats();
    check(propagated && consumer.calls == 1 && counts.emitted == 0 &&
              counts.failed_deliveries == 1,
          "consumer exception is propagated and uncertain delivery not retried");
}

void test_idle_sources_and_shutdown_flush() {
    SensorAcquisition a(std::make_unique<IdleSensor>(SensorId::A), 2);
    SensorAcquisition b(std::make_unique<IdleSensor>(SensorId::B), 2);
    SensorAcquisition c(std::make_unique<IdleSensor>(SensorId::C), 2);
    ConcurrentConsumer consumer;
    MergeStage merge({&a, &b, &c}, consumer, {30ms, 4});
    merge.start();
    const auto observed = std::chrono::steady_clock::now();
    b.stats().received.fetch_add(1, std::memory_order_relaxed);
    SampleBatch first;
    first.push_back(make_sample(SensorId::B, observed, 10));
    check(b.queue().try_submit(std::move(first)),
          "idle B sample accepted by queue");
    check(consumer.wait_for_count(1),
          "idle A and C did not block holdback expiry for B");
    const auto after_holdback = consumer.snapshot();
    check(after_holdback.size() == 1 &&
              after_holdback[0].host_timestamp == observed &&
              std::get<NetworkReading>(after_holdback[0].reading).sequence == 10,
          "idle release preserves exact B sample");

    b.stats().received.fetch_add(1, std::memory_order_relaxed);
    SampleBatch second;
    second.push_back(make_sample(SensorId::B, observed + 10s, 11));
    check(b.queue().try_submit(std::move(second)),
          "future B sample accepted for shutdown flush");
    a.queue().close();
    b.queue().close();
    c.queue().close();
    merge.join();
    const auto output = consumer.snapshot();
    check(output.size() == 2 &&
              std::get<NetworkReading>(output[0].reading).sequence == 10 &&
              std::get<NetworkReading>(output[1].reading).sequence == 11 &&
              output[0].host_timestamp < output[1].host_timestamp &&
              merge.stats().emitted == 2 &&
              merge.stats().late_dropped == 0 &&
              merge.stats().overflow_dropped == 0 &&
              b.stats().received.load() == 2 &&
              b.stats().dropped.load() == 0,
          "closed idle queues flush held sample without lost accounting");
}

template <typename Predicate>
void wait_until(Predicate condition, const char* message) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!condition()) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error(message);
        std::this_thread::sleep_for(1ms);
    }
}

void test_threaded_shutdown_draining() {
    SensorAcquisition a(std::make_unique<MockAnalogSensor>(
                            AnalogMockConfig{0, 0ms, 5}), 64);
    auto b_sensor = std::make_unique<SensorB>();
    auto* b_receiver = b_sensor.get();
    SensorAcquisition b(std::move(b_sensor), 64);
    SensorAcquisition c(std::make_unique<MockSerialSensor>(
                            SerialMockConfig{2, 3}), 64);
    CollectConsumer consumer;
    MergeStage merge({&a, &b, &c}, consumer, {30ms, 8192});
    merge.start();
    a.start();
    b.start();
    c.start();
    try {
        wait_until([&] { return b_receiver->bound_port() != 0; },
                   "B receiver did not bind");
        MockBSenderConfig sender;
        sender.port = b_receiver->bound_port();
        sender.packet_limit = 30;
        sender.loss_every = 0;
        sender.reorder_every = 0;
        sender.duplicate_every = 0;
        sender.malformed_every = 0;
        std::atomic<bool> sender_stop{false};
        MockBSender(sender).run(sender_stop);
        a.join();
        c.join();
        b.stop();
        merge.join();
    } catch (...) {
        a.request_stop();
        b.request_stop();
        c.request_stop();
        try { a.join(); } catch (...) {}
        try { b.join(); } catch (...) {}
        try { c.join(); } catch (...) {}
        try { merge.join(); } catch (...) {}
        throw;
    }

    check(!consumer.samples.empty(), "threaded merge produced samples");
    for (std::size_t i = 1; i < consumer.samples.size(); ++i) {
        const auto& previous = consumer.samples[i - 1];
        const auto& current = consumer.samples[i];
        check(previous.host_timestamp <= current.host_timestamp,
              "merged stream has nondecreasing host timestamps");
        if (previous.host_timestamp == current.host_timestamp) {
            check(previous.sensor <= current.sensor,
                  "merged stream uses sensor tie order");
        }
    }
    const auto count = [&](SensorId id) {
        return std::count_if(consumer.samples.begin(), consumer.samples.end(),
                             [id](const Sample& sample) {
                                 return sample.sensor == id;
                             });
    };
    const auto a_count = count(SensorId::A);
    const auto b_count = count(SensorId::B);
    const auto c_count = count(SensorId::C);
    check(a_count > 0 && b_count > 0 && c_count > 0,
          "all three sensors reached merged stream");
    check(c.stats().rejected.load() == 1 &&
              a.stats().received.load() == 5000 &&
              b.stats().received.load() == 30 &&
              c.stats().received.load() == 2,
          "all sensor acquisition counts preserved");
    check(static_cast<std::uint64_t>(consumer.samples.size()) ==
              merge.stats().emitted &&
              static_cast<std::uint64_t>(a_count) +
                      a.stats().dropped.load() ==
                  a.stats().received.load() &&
              static_cast<std::uint64_t>(b_count) +
                      b.stats().dropped.load() ==
                  b.stats().received.load() &&
              static_cast<std::uint64_t>(c_count) +
                      c.stats().dropped.load() ==
                  c.stats().received.load(),
          "shutdown drains or accounts for each valid sample");
    std::cout << "Merged A=" << a_count << " B=" << b_count
              << " C=" << c_count << " C_rejected="
              << c.stats().rejected.load() << " late_dropped="
              << merge.stats().late_dropped << '\n';
}

}  // namespace

int main() {
    try {
        test_default_holdback_boundary();
        test_late_analog_callback_drops_whole_batch();
        test_ordering_and_ties();
        test_equal_timestamp_after_emission_is_not_late();
        test_late_and_bounded_capacity();
        test_analog_and_serial_metadata();
        test_b_arrival_order_through_merge();
        test_queue_overflow_accounts_analog_block();
        test_concurrent_stats_snapshot();
        test_consumer_exception();
        test_idle_sources_and_shutdown_flush();
        test_threaded_shutdown_draining();
        std::cout << "Merge tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Merge test failed: " << error.what() << '\n';
        return 1;
    }
}
