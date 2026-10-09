#include "sirenopt/merge.hpp"
#include "sirenopt/mock_sensors.hpp"
#include "sirenopt/sensor_b.hpp"

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace {

using namespace sirenopt;
using namespace std::chrono_literals;

volatile std::sig_atomic_t stop_requested = 0;

void on_sigint(int) { stop_requested = 1; }

constexpr std::array<const char*, 3> sensor_names{"A", "B", "C"};

struct ConsumerSnapshot {
    std::array<std::uint64_t, 3> emitted{};
    std::uint64_t lag_sum_us{0};
    std::uint64_t last_lag_us{0};
};

class CountingConsumer final : public IMergedConsumer {
public:
    void consume(Sample&& sample) noexcept override {
        const auto index = static_cast<std::size_t>(sample.sensor);
        const auto elapsed = std::chrono::steady_clock::now() -
                             sample.host_timestamp;
        const auto lag = std::chrono::duration_cast<std::chrono::microseconds>(
            elapsed).count();
        const auto lag_us = static_cast<std::uint64_t>(lag > 0 ? lag : 0);
        lag_sum_us_.fetch_add(lag_us, std::memory_order_relaxed);
        last_lag_us_.store(lag_us, std::memory_order_relaxed);
        emitted_[index].fetch_add(1, std::memory_order_relaxed);
    }

    ConsumerSnapshot snapshot() const noexcept {
        ConsumerSnapshot result;
        for (std::size_t i = 0; i < result.emitted.size(); ++i) {
            result.emitted[i] = emitted_[i].load(std::memory_order_relaxed);
        }
        result.lag_sum_us = lag_sum_us_.load(std::memory_order_relaxed);
        result.last_lag_us = last_lag_us_.load(std::memory_order_relaxed);
        return result;
    }

private:
    std::array<std::atomic<std::uint64_t>, 3> emitted_{};
    std::atomic<std::uint64_t> lag_sum_us_{0};
    std::atomic<std::uint64_t> last_lag_us_{0};
};

struct PipelineSnapshot {
    ConsumerSnapshot consumer;
    std::array<SensorStatsSnapshot, 3> sensors;
    MergeStats merge;
};

PipelineSnapshot snapshot(const CountingConsumer& consumer,
                          const std::array<SensorAcquisition*, 3>& sources,
                          const MergeStage& merge) {
    return {consumer.snapshot(),
            {sources[0]->stats().snapshot(), sources[1]->stats().snapshot(),
             sources[2]->stats().snapshot()},
            merge.stats()};
}

std::uint64_t total(const std::array<std::uint64_t, 3>& values) {
    return values[0] + values[1] + values[2];
}

void print_report(const PipelineSnapshot& current,
                  const PipelineSnapshot& previous) {
    const auto& now = current.consumer;
    const auto& before = previous.consumer;
    std::cout << "interval | emitted";
    for (std::size_t i = 0; i < 3; ++i) {
        std::cout << ' ' << sensor_names[i] << '='
                  << std::setw(6) << now.emitted[i] - before.emitted[i];
    }
    const auto interval_count = total(now.emitted) - total(before.emitted);
    if (interval_count) {
        const auto lag_delta = now.lag_sum_us - before.lag_sum_us;
        std::cout << " | lag ms avg=" << std::fixed << std::setprecision(2)
                  << std::setw(6)
                  << static_cast<double>(lag_delta) / interval_count / 1000.0
                  << " last=" << std::setw(6)
                  << static_cast<double>(now.last_lag_us) / 1000.0;
    } else {
        std::cout << " | lag ms avg=   n/a last=   n/a";
    }
    std::uint64_t total_drops = 0;
    for (std::size_t i = 0; i < 3; ++i) {
        total_drops += current.sensors[i].dropped - previous.sensors[i].dropped;
    }
    std::cout << " | drops=" << total_drops << '\n';

    bool has_events = false;
    const auto event = [&](const char* name, std::uint64_t count) {
        if (!count) return;
        if (!has_events) std::cout << "         | events";
        std::cout << ' ' << name << '=' << count;
        has_events = true;
    };
    for (std::size_t i = 0; i < 3; ++i) {
        const auto& sensor = current.sensors[i];
        const auto& old = previous.sensors[i];
        const auto gaps = sensor.gaps - old.gaps;
        const auto rejected = sensor.rejected - old.rejected;
        const auto duplicates = sensor.duplicates - old.duplicates;
        const auto late = sensor.late - old.late;
        if (!(gaps || rejected || duplicates || late)) continue;
        if (!has_events) std::cout << "         | events";
        std::cout << ' ' << sensor_names[i] << '[';
        bool first = true;
        const auto field = [&](const char* name, std::uint64_t count) {
            if (!count) return;
            if (!first) std::cout << ' ';
            std::cout << name << '=' << count;
            first = false;
        };
        field("gap", gaps);
        field("reject", rejected);
        field("dup", duplicates);
        field("late", late);
        std::cout << ']';
        has_events = true;
    }
    event("merge_late_drop", current.merge.late_dropped -
                                 previous.merge.late_dropped);
    event("merge_overflow_drop", current.merge.overflow_dropped -
                                     previous.merge.overflow_dropped);
    event("delivery_uncertain", current.merge.failed_deliveries -
                                     previous.merge.failed_deliveries);
    if (has_events) std::cout << '\n';
    std::cout << std::flush;
}

bool print_final(const PipelineSnapshot& final) {
    std::uint64_t received = 0;
    std::uint64_t emitted = 0;
    std::uint64_t dropped = 0;
    std::uint64_t rejected = 0;
    bool consistent = true;
    std::cout << "final (valid received = emitted + dropped):\n";
    std::cout << "  sensor" << std::setw(13) << "received"
              << std::setw(11) << "emitted" << std::setw(11) << "dropped"
              << std::setw(12) << "rejected" << '\n';
    for (std::size_t i = 0; i < 3; ++i) {
        const auto& sensor = final.sensors[i];
        const auto output = final.consumer.emitted[i];
        std::cout << "  " << sensor_names[i] << std::setw(18)
                  << sensor.received << std::setw(11) << output
                  << std::setw(11) << sensor.dropped << std::setw(12)
                  << sensor.rejected << '\n';
        received += sensor.received;
        emitted += output;
        dropped += sensor.dropped;
        rejected += sensor.rejected;
        consistent &= sensor.received == output + sensor.dropped;
    }
    consistent &= emitted == final.merge.emitted;
    consistent &= final.merge.failed_deliveries == 0;
    std::cout << "  total received=" << received << " emitted=" << emitted
              << " dropped=" << dropped << " rejected=" << rejected << '\n';
    const auto& b = final.sensors[1];
    std::cout << "  B sequence gaps=" << b.gaps
              << " duplicates=" << b.duplicates << " late=" << b.late
              << " out_of_order=" << b.out_of_order
              << " recovered=" << b.recovered << '\n';
    std::cout << "  merge emitted=" << final.merge.emitted
              << " late_dropped=" << final.merge.late_dropped
              << " overflow_dropped=" << final.merge.overflow_dropped
              << " delivery_uncertain=" << final.merge.failed_deliveries
              << '\n';
    std::cout << "  accounting=" << (consistent ? "ok" : "incomplete")
              << '\n' << std::flush;
    return consistent;
}

}  // namespace

int main(int argc, char** argv) {
    MergeConfig merge_config;
    if (argc != 1) {
        if (argc != 3 || std::string_view(argv[1]) != "--holdback-ms") {
            std::cerr << "usage: sirenopt [--holdback-ms N]\n";
            return 2;
        }
        const std::string_view value(argv[2]);
        std::int64_t milliseconds = 0;
        const auto parsed = std::from_chars(value.data(),
                                            value.data() + value.size(),
                                            milliseconds);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != value.data() + value.size() || milliseconds < 0) {
            std::cerr << "holdback must be a nonnegative whole number of ms\n";
            return 2;
        }
        merge_config.holdback = std::chrono::milliseconds(milliseconds);
    }
    std::signal(SIGINT, on_sigint);
    SensorAcquisition a(std::make_unique<MockAnalogSensor>());
    auto b_sensor = std::make_unique<SensorB>();
    auto* b_receiver = b_sensor.get();
    SensorAcquisition b(std::move(b_sensor));
    SensorAcquisition c(std::make_unique<MockSerialSensor>());
    const std::array<SensorAcquisition*, 3> sources{&a, &b, &c};
    CountingConsumer consumer;
    MergeStage merge(sources, consumer, merge_config);
    std::atomic<bool> sender_stop{false};
    std::atomic<bool> sender_finished{false};
    std::exception_ptr sender_error;
    std::thread sender;
    std::exception_ptr first_error;

    try {
        merge.start();
        a.start();
        b.start();
        c.start();
        const auto bind_deadline = std::chrono::steady_clock::now() + 2s;
        while (b_receiver->bound_port() == 0 &&
               std::chrono::steady_clock::now() < bind_deadline &&
               !stop_requested) {
            std::this_thread::sleep_for(1ms);
        }
        if (!stop_requested && b_receiver->bound_port() == 0) {
            throw std::runtime_error("Sensor B did not bind UDP");
        }
        if (!stop_requested) {
            MockBSenderConfig config;
            config.port = b_receiver->bound_port();
            sender = std::thread([&, config] {
                try {
                    MockBSender(config).run(sender_stop);
                } catch (...) {
                    sender_error = std::current_exception();
                }
                sender_finished.store(true, std::memory_order_release);
            });
        }

        std::cout << "SirenOpt running (holdback="
                  << merge_config.holdback.count()
                  << "ms); press Ctrl-C to stop\n" << std::flush;
        auto previous = snapshot(consumer, sources, merge);
        auto next_report = std::chrono::steady_clock::now() + 1s;
        while (!stop_requested) {
            if (sender_finished.load(std::memory_order_acquire)) break;
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_report) {
                const auto current = snapshot(consumer, sources, merge);
                print_report(current, previous);
                previous = current;
                next_report = now + 1s;
            }
            std::this_thread::sleep_for(10ms);
        }
    } catch (...) {
        first_error = std::current_exception();
    }

    sender_stop.store(true, std::memory_order_relaxed);
    if (sender.joinable()) sender.join();
    for (auto* source : sources) source->request_stop();
    for (auto* source : sources) {
        try {
            source->join();
        } catch (...) {
            if (!first_error) first_error = std::current_exception();
        }
    }
    try {
        merge.join();
    } catch (...) {
        if (!first_error) first_error = std::current_exception();
    }
    if (sender_error && !first_error) first_error = sender_error;

    const bool consistent = print_final(snapshot(consumer, sources, merge));
    if (first_error) {
        try {
            std::rethrow_exception(first_error);
        } catch (const std::exception& error) {
            std::cerr << "SirenOpt error: " << error.what() << '\n';
        }
    }
    return first_error || !consistent ? 1 : 0;
}
