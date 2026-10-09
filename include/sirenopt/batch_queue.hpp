#pragma once

#include "sirenopt/sensor.hpp"

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>

namespace sirenopt {

class BoundedBatchQueue final : public ISampleSink {
public:
    explicit BoundedBatchQueue(std::size_t capacity_batches,
                               SensorStats& stats);

    bool try_submit(SampleBatch&& batch) noexcept override;
    std::optional<SampleBatch> try_pop();
    std::optional<SampleBatch> wait_pop();
    void close();

    std::size_t capacity_batches() const noexcept { return capacity_batches_; }
    std::size_t size_batches() const;
    bool closed_and_empty() const;

private:
    const std::size_t capacity_batches_;
    SensorStats& stats_;
    mutable std::mutex mutex_;
    std::condition_variable available_;
    std::vector<std::optional<SampleBatch>> slots_;
    std::size_t head_{0};
    std::size_t size_{0};
    bool closed_{false};
};

}  // namespace sirenopt
