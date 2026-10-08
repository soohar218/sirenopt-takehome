#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace sirenopt {

enum class SensorId : std::uint8_t { A, B, C };

using HostTime = std::chrono::steady_clock::time_point;

struct AnalogReading {
    double value;
};

struct NetworkReading {
    double value;
    std::uint32_t sequence;
};

struct SerialReading {
    double temperature;
    double pressure;
};

using Reading = std::variant<AnalogReading, NetworkReading, SerialReading>;

struct Sample {
    SensorId sensor;
    HostTime host_timestamp;
    std::optional<std::uint64_t> device_timestamp_ns;
    Reading reading;
};

// A callback may submit 1,000 analog samples as one batch; B and C use one.
using SampleBatch = std::vector<Sample>;

struct SensorStatsSnapshot {
    std::uint64_t received;
    std::uint64_t dropped;
    std::uint64_t rejected;
    std::uint64_t gaps;
    std::uint64_t duplicates;
    std::uint64_t late;
    std::uint64_t out_of_order;
    std::uint64_t recovered;
};

struct SensorStats {
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> dropped{0};
    std::atomic<std::uint64_t> rejected{0};
    std::atomic<std::uint64_t> gaps{0};
    std::atomic<std::uint64_t> duplicates{0};
    std::atomic<std::uint64_t> late{0};
    std::atomic<std::uint64_t> out_of_order{0};
    std::atomic<std::uint64_t> recovered{0};

    SensorStatsSnapshot snapshot() const noexcept;
};

}  // namespace sirenopt
