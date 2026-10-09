#pragma once

#include "sirenopt/acquisition.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <optional>
#include <thread>
#include <tuple>

namespace sirenopt {

class IMergedConsumer {
public:
    virtual ~IMergedConsumer() = default;
    virtual void consume(Sample&& sample) = 0;
};

struct MergeStats {
    std::uint64_t emitted{0};
    std::uint64_t late_dropped{0};
    std::uint64_t overflow_dropped{0};
    std::uint64_t failed_deliveries{0};  // consume() threw; delivery is uncertain.
};

struct MergeConfig {
    std::chrono::milliseconds holdback{10};
    std::size_t capacity_samples{8192};
};

class MergeBuffer {
public:
    explicit MergeBuffer(MergeConfig config = {});

    void push(Sample&& sample, SensorStats& source_stats);
    void emit_ready(HostTime now, IMergedConsumer& consumer);
    void emit_all(IMergedConsumer& consumer);

    // Thread-safe, per-counter snapshot; fields are not a single atomic instant.
    MergeStats stats() const noexcept;
    std::size_t size_samples() const noexcept { return pending_.size(); }

private:
    struct Key {
        HostTime timestamp;
        SensorId sensor;
        std::uint64_t input_order;

        bool operator<(const Key& other) const noexcept {
            return std::tie(timestamp, sensor, input_order) <
                   std::tie(other.timestamp, other.sensor,
                            other.input_order);
        }
    };

    void emit_first(IMergedConsumer& consumer);

    MergeConfig config_;
    std::map<Key, Sample> pending_;
    std::array<std::uint64_t, 3> next_order_{};
    std::optional<HostTime> last_emitted_timestamp_;
    std::atomic<std::uint64_t> emitted_{0};
    std::atomic<std::uint64_t> late_dropped_{0};
    std::atomic<std::uint64_t> overflow_dropped_{0};
    std::atomic<std::uint64_t> failed_deliveries_{0};
};

class MergeStage {
public:
    MergeStage(std::array<SensorAcquisition*, 3> sources,
               IMergedConsumer& consumer, MergeConfig config = {});
    // Sources and consumer must outlive this stage. Destruction before join is
    // an emergency abort: already queued samples may remain unaccounted for.
    ~MergeStage();

    MergeStage(const MergeStage&) = delete;
    MergeStage& operator=(const MergeStage&) = delete;

    void start();
    void join();  // Stop/join producers first; drains queues and flushes held samples.
    MergeStats stats() const noexcept { return buffer_.stats(); }

private:
    void run();

    std::array<SensorAcquisition*, 3> sources_;
    IMergedConsumer& consumer_;
    MergeBuffer buffer_;
    std::atomic<bool> abort_{false};
    std::thread worker_;
    std::exception_ptr worker_error_;
    bool started_{false};
};

}  // namespace sirenopt
