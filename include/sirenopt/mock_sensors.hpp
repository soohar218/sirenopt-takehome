#pragma once

#include "sirenopt/sensor.hpp"

#include <cstdint>
#include <optional>
#include <string_view>

namespace sirenopt {

constexpr std::size_t kAnalogBlockSize = 1000;
constexpr std::uint64_t kAnalogSamplePeriodNs = 10000;

SampleBatch make_analog_block(std::uint64_t block_index,
                              HostTime callback_arrival);

struct AnalogMockConfig {
    std::uint32_t late_every{25};
    std::chrono::milliseconds late_delay{15};
    std::uint64_t block_limit{0};  // Zero means run until stopped.
};

class MockAnalogSensor final : public ISensor {
public:
    explicit MockAnalogSensor(AnalogMockConfig config = {});
    SensorId id() const noexcept override { return SensorId::A; }
    void run(ISampleSink& sink, const std::atomic<bool>& stop_requested,
             SensorStats& stats) override;

private:
    AnalogMockConfig config_;
};

std::optional<SerialReading> parse_serial_line(std::string_view line);

struct SerialMockConfig {
    std::uint32_t corrupt_every{17};
    std::uint64_t line_limit{0};  // Zero means run until stopped.
};

class MockSerialSensor final : public ISensor {
public:
    explicit MockSerialSensor(SerialMockConfig config = {});
    SensorId id() const noexcept override { return SensorId::C; }
    void run(ISampleSink& sink, const std::atomic<bool>& stop_requested,
             SensorStats& stats) override;

private:
    SerialMockConfig config_;
};

}  // namespace sirenopt
