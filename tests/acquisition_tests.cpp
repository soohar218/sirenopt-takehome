#include "sirenopt/acquisition.hpp"
#include "sirenopt/sensor_b.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using namespace sirenopt;
using namespace std::chrono_literals;

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

SampleBatch make_batch(std::size_t count, std::uint32_t first_sequence = 0) {
    SampleBatch batch;
    batch.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        batch.push_back({SensorId::B,
                         HostTime{} + std::chrono::microseconds(
                                          first_sequence + i),
                         static_cast<std::uint64_t>(first_sequence + i),
                         NetworkReading{static_cast<double>(first_sequence + i),
                                        first_sequence + static_cast<std::uint32_t>(i)}});
    }
    return batch;
}

std::uint32_t first_sequence(const SampleBatch& batch) {
    return std::get<NetworkReading>(batch.at(0).reading).sequence;
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

void test_capacity_fifo_and_sample_accounting() {
    SensorStats stats;
    BoundedBatchQueue queue(2, stats);
    check(queue.capacity_batches() == 2, "capacity measured in batches");
    check(queue.try_submit(make_batch(2, 10)), "first batch accepted");
    check(queue.try_submit(make_batch(1, 20)), "second batch accepted");
    check(queue.size_batches() == 2, "two occupied batch slots");
    check(!queue.try_submit(make_batch(1000, 30)), "full queue drops newest");
    check(stats.snapshot().dropped == 1000, "drop counts 1000 samples");
    auto first = queue.try_pop();
    auto second = queue.try_pop();
    check(first && second && first->size() == 2 && second->size() == 1 &&
              first_sequence(*first) == 10 && first_sequence(*second) == 20,
          "FIFO batches retained");
    check((*first)[0].host_timestamp == HostTime{} + 10us &&
              (*second)[0].host_timestamp == HostTime{} + 20us,
          "original host timestamps retained");
    check(!queue.try_pop(), "empty queue returns no batch");
    check(queue.try_submit(make_batch(1, 40)), "ring slot reused");
    queue.close();
    auto drained = queue.wait_pop();
    check(drained && first_sequence(*drained) == 40,
          "close preserves queued batch");
    check(!queue.wait_pop(), "closed empty queue wakes consumer");
    check(!queue.try_submit(make_batch(2, 50)),
          "closed queue rejects new batch");
    check(stats.snapshot().dropped == 1002,
          "closed rejection counted in samples");
}

void test_concurrent_fifo() {
    SensorStats stats;
    BoundedBatchQueue queue(1000, stats);
    std::vector<std::uint32_t> accepted;
    accepted.reserve(1000);
    std::vector<std::uint32_t> consumed;
    consumed.reserve(1000);
    std::thread consumer([&] {
        while (auto batch = queue.wait_pop()) {
            consumed.push_back(first_sequence(*batch));
        }
    });
    std::thread producer([&] {
        for (std::uint32_t i = 0; i < 1000; ++i) {
            if (queue.try_submit(make_batch(1, i))) accepted.push_back(i);
        }
    });
    producer.join();
    queue.close();
    consumer.join();
    check(consumed == accepted &&
              stats.snapshot().dropped == 1000 - accepted.size(),
          "concurrent FIFO and drop accounting");
}

void test_stalled_consumer() {
    SensorStats stats;
    BoundedBatchQueue queue(1, stats);
    check(queue.try_submit(make_batch(1, 1)), "initial batch accepted");
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool processing = false;
    bool release = false;
    std::thread consumer([&] {
        auto batch = queue.wait_pop();
        {
            std::lock_guard<std::mutex> lock(gate_mutex);
            processing = batch.has_value();
        }
        gate_cv.notify_one();
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait(lock, [&] { return release; });
    });
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        const bool ready = gate_cv.wait_for(lock, 2s,
                                            [&] { return processing; });
        if (!ready) {
            release = true;
            lock.unlock();
            gate_cv.notify_one();
            consumer.join();
            throw std::runtime_error("consumer did not start processing");
        }
    }
    std::promise<bool> submitted;
    auto finished = submitted.get_future();
    std::thread producer([&] {
        const bool accepted = queue.try_submit(make_batch(1, 2));
        const bool rejected = !queue.try_submit(make_batch(1000, 3));
        submitted.set_value(accepted && rejected);
    });
    const bool completed_while_stalled = finished.wait_for(500ms) ==
                                         std::future_status::ready;
    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        release = true;
    }
    gate_cv.notify_one();
    producer.join();
    consumer.join();
    check(completed_while_stalled && finished.get(),
          "producer did not wait for slow consumer");
    auto pending = queue.try_pop();
    check(pending && first_sequence(*pending) == 2 &&
              stats.snapshot().dropped == 1000,
          "stalled consumer retained oldest and dropped newest");
}

struct WorkerState {
    std::atomic<bool> entered{false};
    std::thread::id thread_id;
};

class WaitingSensor final : public ISensor {
public:
    explicit WaitingSensor(std::shared_ptr<WorkerState> state)
        : state_(std::move(state)) {}

    SensorId id() const noexcept override { return SensorId::B; }

    void run(ISampleSink& sink, const std::atomic<bool>& stop_requested,
             SensorStats& stats) override {
        state_->thread_id = std::this_thread::get_id();
        stats.received.fetch_add(3, std::memory_order_relaxed);
        sink.try_submit(make_batch(3, 100));
        state_->entered.store(true, std::memory_order_release);
        std::mutex mutex;
        std::condition_variable cv;
        std::unique_lock<std::mutex> lock(mutex);
        while (!stop_requested.load(std::memory_order_relaxed)) {
            cv.wait_for(lock, 10ms);
        }
    }

private:
    std::shared_ptr<WorkerState> state_;
};

void test_workers_and_shutdown() {
    auto first_state = std::make_shared<WorkerState>();
    auto second_state = std::make_shared<WorkerState>();
    SensorAcquisition first(std::make_unique<WaitingSensor>(first_state), 1);
    SensorAcquisition second(std::make_unique<WaitingSensor>(second_state), 1);
    first.start();
    second.start();
    wait_until([&] {
        return first_state->entered.load(std::memory_order_acquire) &&
               second_state->entered.load(std::memory_order_acquire);
    }, "workers did not start");
    check(first_state->thread_id != second_state->thread_id &&
              first_state->thread_id != std::this_thread::get_id(),
          "independent acquisition threads");
    first.request_stop();
    second.request_stop();
    first.join();
    second.join();
    auto first_batch = first.queue().wait_pop();
    auto second_batch = second.queue().wait_pop();
    check(first_batch && second_batch && first_batch->size() == 3 &&
              second_batch->size() == 3 &&
              !first.queue().wait_pop() && !second.queue().wait_pop(),
          "shutdown closes and drains both queues");
    check(first.stats().snapshot().received == 3 &&
              first.stats().snapshot().dropped == 0 &&
              second.stats().snapshot().received == 3 &&
              second.stats().snapshot().dropped == 0,
          "shutdown preserves sample accounting");
}

void test_sensor_b_overflow() {
    auto sensor = std::make_unique<SensorB>(SensorBConfig{});
    auto* receiver = sensor.get();
    SensorAcquisition acquisition(std::move(sensor), 1);
    acquisition.start();
    try {
        wait_until([&] { return receiver->bound_port() != 0; },
                   "Sensor B worker did not bind UDP");
        MockBSenderConfig config;
        config.port = receiver->bound_port();
        config.packet_limit = 8;
        config.loss_every = 0;
        config.reorder_every = 0;
        config.duplicate_every = 0;
        config.malformed_every = 0;
        std::atomic<bool> sender_stop{false};
        MockBSender(config).run(sender_stop);
        wait_until([&] { return acquisition.stats().received.load() == 8; },
                   "Sensor B worker did not receive packets");
        acquisition.stop();
        auto batch = acquisition.queue().wait_pop();
        check(batch && first_sequence(*batch) == 0 &&
                  !acquisition.queue().wait_pop(),
              "drop newest keeps first UDP packet");
        const auto counts = acquisition.stats().snapshot();
        check(counts.received == 8 && counts.dropped == 7 &&
                  counts.rejected == 0,
              "Sensor B queue overflow accounted in samples");
        std::cout << "Sensor B queue capacity=1 batch: received="
                  << counts.received << " dropped=" << counts.dropped
                  << " retained=" << first_sequence(*batch) << '\n';
    } catch (...) {
        acquisition.request_stop();
        try {
            acquisition.join();
        } catch (...) {
        }
        throw;
    }
}

}  // namespace

int main() {
    try {
        test_capacity_fifo_and_sample_accounting();
        test_concurrent_fifo();
        test_stalled_consumer();
        test_workers_and_shutdown();
        test_sensor_b_overflow();
        std::cout << "Acquisition tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Acquisition test failed: " << error.what() << '\n';
        return 1;
    }
}
