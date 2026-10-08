#pragma once

#include "sirenopt/types.hpp"

namespace sirenopt {

class ISampleSink {
public:
    virtual ~ISampleSink() = default;

    // The caller retains ownership of batch if submission fails.
    // Implementations must return immediately even when full or contended.
    virtual bool try_submit(SampleBatch&& batch) noexcept = 0;
};

class ISensor {
public:
    virtual ~ISensor() = default;

    virtual SensorId id() const noexcept = 0;

    // Called on this sensor's acquisition thread; returns after stop is set.
    virtual void run(ISampleSink& sink,
                     const std::atomic<bool>& stop_requested,
                     SensorStats& stats) = 0;
};

}  // namespace sirenopt
