#include "sirenopt/merge.hpp"
#include "sirenopt/mock_sensors.hpp"
#include "sirenopt/sensor_b.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace {

using namespace sirenopt;
using namespace std::chrono_literals;

constexpr std::size_t kLagBuckets = 100001;  // 10 us bins, overflow at 1 s.
constexpr std::array<char, 3> kNames{'A', 'B', 'C'};

double cpu_seconds(const rusage& usage) {
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
           usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
}

class LagConsumer final : public IMergedConsumer {
public:
    LagConsumer() {
        for (auto& bins : histogram_) bins.resize(kLagBuckets);
    }

    void consume(Sample&& sample) noexcept override {
        const auto index = static_cast<std::size_t>(sample.sensor);
        const auto elapsed = std::chrono::steady_clock::now() -
                             sample.host_timestamp;
        const auto us = std::max<std::int64_t>(
            0, std::chrono::duration_cast<std::chrono::microseconds>(elapsed)
                   .count());
        ++histogram_[index][std::min<std::size_t>(
            static_cast<std::size_t>(us / 10), kLagBuckets - 1)];
        max_us_[index] = std::max(max_us_[index], static_cast<std::uint64_t>(us));
        emitted_[index].fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t count(std::size_t index) const noexcept {
        return emitted_[index].load(std::memory_order_relaxed);
    }

    double percentile_ms(std::size_t index, double fraction) const {
        const auto target = static_cast<std::uint64_t>(
            std::max(1.0, std::ceil(static_cast<double>(count(index)) * fraction)));
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < kLagBuckets; ++i) {
            seen += histogram_[index][i];
            if (seen >= target) return i / 100.0;
        }
        return 0;
    }

    double max_ms(std::size_t index) const noexcept {
        return max_us_[index] / 1000.0;
    }

private:
    std::array<std::atomic<std::uint64_t>, 3> emitted_{};
    std::array<std::vector<std::uint64_t>, 3> histogram_;
    std::array<std::uint64_t, 3> max_us_{};
};

}  // namespace

int main(int argc, char** argv) {
    try {
        int seconds = 60;
        if (argc > 2) throw std::invalid_argument("usage: stability_benchmark [seconds]");
        if (argc == 2) {
            const std::string_view input(argv[1]);
            const auto parsed = std::from_chars(input.data(), input.data() + input.size(), seconds);
            if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() ||
                seconds <= 0) {
                throw std::invalid_argument("duration must be positive whole seconds");
            }
        }

        SensorAcquisition a(std::make_unique<MockAnalogSensor>());
        auto b_sensor = std::make_unique<SensorB>();
        auto* receiver = b_sensor.get();
        SensorAcquisition b(std::move(b_sensor));
        SensorAcquisition c(std::make_unique<MockSerialSensor>());
        const std::array<SensorAcquisition*, 3> sources{&a, &b, &c};
        LagConsumer consumer;
        MergeStage merge(sources, consumer);  // Production default: 10 ms.
        std::atomic<bool> sender_stop{false};
        std::exception_ptr sender_error;

        rusage usage_start{};
        getrusage(RUSAGE_SELF, &usage_start);
        merge.start();
        a.start();
        b.start();
        c.start();
        const auto bind_deadline = std::chrono::steady_clock::now() + 2s;
        while (receiver->bound_port() == 0 &&
               std::chrono::steady_clock::now() < bind_deadline) {
            std::this_thread::sleep_for(1ms);
        }
        if (receiver->bound_port() == 0) {
            throw std::runtime_error("Sensor B did not bind UDP");
        }
        MockBSenderConfig sender_config;
        sender_config.port = receiver->bound_port();
        std::thread sender([&] {
            try {
                MockBSender(sender_config).run(sender_stop);
            } catch (...) {
                sender_error = std::current_exception();
            }
        });

        const auto begin = std::chrono::steady_clock::now();
        auto window_start = begin;
        std::array<std::uint64_t, 3> previous{};
        std::array<std::size_t, 3> peak_depth{};
        while (std::chrono::steady_clock::now() - begin <
               std::chrono::seconds(seconds)) {
            std::this_thread::sleep_for(10ms);
            const auto now = std::chrono::steady_clock::now();
            for (std::size_t i = 0; i < 3; ++i) {
                peak_depth[i] = std::max(peak_depth[i],
                                         sources[i]->queue().size_batches());
            }
            if (now - window_start >= 10s) {
                const auto elapsed = std::chrono::duration<double>(now - window_start).count();
                std::cout << "window_s=" << std::fixed << std::setprecision(1)
                          << std::chrono::duration<double>(now - begin).count();
                for (std::size_t i = 0; i < 3; ++i) {
                    const auto count = consumer.count(i);
                    std::cout << ' ' << kNames[i] << "_per_s=" << std::setprecision(1)
                              << (count - previous[i]) / elapsed
                              << " q_peak=" << peak_depth[i];
                    previous[i] = count;
                    peak_depth[i] = 0;
                }
                std::cout << " merge_late=" << merge.stats().late_dropped << '\n';
                window_start = now;
            }
        }

        sender_stop.store(true, std::memory_order_relaxed);
        sender.join();
        for (auto* source : sources) source->request_stop();
        for (auto* source : sources) source->join();
        merge.join();
        if (sender_error) std::rethrow_exception(sender_error);

        rusage usage_end{};
        getrusage(RUSAGE_SELF, &usage_end);
        const auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - begin).count();
        const auto merge_stats = merge.stats();
        std::uint64_t received = 0, emitted = 0, dropped = 0;
        bool balanced = merge_stats.failed_deliveries == 0;
        for (std::size_t i = 0; i < 3; ++i) {
            const auto stats = sources[i]->stats().snapshot();
            const auto output = consumer.count(i);
            received += stats.received;
            emitted += output;
            dropped += stats.dropped;
            balanced &= stats.received == output + stats.dropped;
            std::cout << kNames[i] << " received=" << stats.received
                      << " emitted=" << output << " per_s=" << std::fixed
                      << std::setprecision(1) << output / elapsed
                      << " dropped=" << stats.dropped
                      << " rejected=" << stats.rejected
                      << " lag_ms[p50,p95,p99,max]=" << std::setprecision(2)
                      << consumer.percentile_ms(i, .50) << ','
                      << consumer.percentile_ms(i, .95) << ','
                      << consumer.percentile_ms(i, .99) << ','
                      << consumer.max_ms(i) << '\n';
        }
        balanced &= emitted == merge_stats.emitted;
        const auto b_stats = b.stats().snapshot();
        std::cout << "B gaps=" << b_stats.gaps << " duplicates="
                  << b_stats.duplicates << " out_of_order="
                  << b_stats.out_of_order << " recovered="
                  << b_stats.recovered << '\n';
        std::cout << "merge late_dropped=" << merge_stats.late_dropped
                  << " overflow_dropped=" << merge_stats.overflow_dropped
                  << " failed_deliveries=" << merge_stats.failed_deliveries
                  << '\n';
        std::cout << "total received=" << received << " emitted=" << emitted
                  << " dropped=" << dropped << " elapsed_s=" << elapsed
                  << " cpu_pct=" << 100 * (cpu_seconds(usage_end) -
                                           cpu_seconds(usage_start)) / elapsed
                  << " accounting=" << (balanced ? "ok" : "incomplete")
                  << '\n';
        return balanced ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "stability benchmark: " << error.what() << '\n';
        return 1;
    }
}
