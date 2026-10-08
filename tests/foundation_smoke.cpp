#include "sirenopt/sensor.hpp"

#include <type_traits>

int main() {
    using namespace sirenopt;

    static_assert(std::is_abstract<ISensor>::value, "Sensor interface required");
    static_assert(std::is_abstract<ISampleSink>::value, "Sink interface required");

    SensorStats stats;
    SampleBatch block;
    block.push_back({SensorId::A, HostTime{}, std::nullopt,
                     AnalogReading{1.0}});
    block.push_back({SensorId::C, HostTime{}, std::nullopt,
                     SerialReading{23.41, 1013.2}});

    const auto counts = stats.snapshot();
    return block.size() == 2 && counts.received == 0 && counts.dropped == 0
               ? 0
               : 1;
}
