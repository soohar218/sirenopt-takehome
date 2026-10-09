#include "sirenopt/mock_sensors.hpp"

#include <cmath>
#include <iomanip>
#include <locale>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace sirenopt {
namespace {

bool parse_number(std::string_view text, double& value) {
    if (text.empty()) return false;
    std::istringstream input{std::string(text)};
    input.imbue(std::locale::classic());
    input >> std::noskipws >> value;
    return static_cast<bool>(input) &&
           input.peek() == std::char_traits<char>::eof() &&
           std::isfinite(value);
}

}  // namespace

SampleBatch make_analog_block(std::uint64_t block_index,
                              HostTime callback_arrival) {
    SampleBatch batch;
    batch.reserve(kAnalogBlockSize);
    const auto first_sample = block_index * kAnalogBlockSize;
    for (std::size_t i = 0; i < kAnalogBlockSize; ++i) {
        const auto sample_index = first_sample + i;
        batch.push_back({SensorId::A, callback_arrival,
                         sample_index * kAnalogSamplePeriodNs,
                         AnalogReading{static_cast<double>(sample_index % 1024) /
                                       1024.0}});
    }
    return batch;
}

MockAnalogSensor::MockAnalogSensor(AnalogMockConfig config) : config_(config) {
    if (config_.late_delay.count() < 0) {
        throw std::invalid_argument("analog late delay cannot be negative");
    }
}

void MockAnalogSensor::run(ISampleSink& sink,
                           const std::atomic<bool>& stop_requested,
                           SensorStats& stats) {
    constexpr auto block_period = std::chrono::milliseconds(10);
    auto next_callback = std::chrono::steady_clock::now() + block_period;
    for (std::uint64_t block = 0;
         !stop_requested.load(std::memory_order_relaxed) &&
         (config_.block_limit == 0 || block < config_.block_limit);
         ++block) {
        std::this_thread::sleep_until(next_callback);
        if (config_.late_every && (block + 1) % config_.late_every == 0) {
            std::this_thread::sleep_for(config_.late_delay);
        }
        if (stop_requested.load(std::memory_order_relaxed)) break;
        const auto callback_arrival = std::chrono::steady_clock::now();
        auto samples = make_analog_block(block, callback_arrival);
        stats.received.fetch_add(samples.size(), std::memory_order_relaxed);
        sink.try_submit(std::move(samples));
        next_callback += block_period;
    }
}

std::optional<SerialReading> parse_serial_line(std::string_view line) {
    if (!line.empty() && line.back() == '\n') line.remove_suffix(1);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (line.size() < 7 || line.substr(0, 2) != "T=") return std::nullopt;
    const auto separator = line.find(",P=");
    if (separator == std::string_view::npos) return std::nullopt;
    double temperature;
    double pressure;
    if (!parse_number(line.substr(2, separator - 2), temperature) ||
        !parse_number(line.substr(separator + 3), pressure)) {
        return std::nullopt;
    }
    return SerialReading{temperature, pressure};
}

MockSerialSensor::MockSerialSensor(SerialMockConfig config) : config_(config) {}

void MockSerialSensor::run(ISampleSink& sink,
                           const std::atomic<bool>& stop_requested,
                           SensorStats& stats) {
    constexpr auto line_period = std::chrono::milliseconds(100);
    auto next_line = std::chrono::steady_clock::now() + line_period;
    for (std::uint64_t line_number = 0;
         !stop_requested.load(std::memory_order_relaxed) &&
         (config_.line_limit == 0 || line_number < config_.line_limit);
         ++line_number) {
        std::this_thread::sleep_until(next_line);
        if (stop_requested.load(std::memory_order_relaxed)) break;
        std::string line;
        if (config_.corrupt_every &&
            (line_number + 1) % config_.corrupt_every == 0) {
            line = "T=corrupt,P=1013.2";
        } else {
            std::ostringstream output;
            output.imbue(std::locale::classic());
            output << std::fixed << std::setprecision(2)
                   << "T=" << 23.41 + static_cast<double>(line_number % 10) * 0.1
                   << ",P=" << 1013.2;
            line = output.str();
        }
        line += '\n';
        const auto line_arrival = std::chrono::steady_clock::now();
        const auto reading = parse_serial_line(line);
        if (!reading) {
            stats.rejected.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats.received.fetch_add(1, std::memory_order_relaxed);
            SampleBatch batch;
            batch.push_back({SensorId::C, line_arrival,
                             line_number * 100000000ull, *reading});
            sink.try_submit(std::move(batch));
        }
        next_line += line_period;
    }
}

}  // namespace sirenopt
