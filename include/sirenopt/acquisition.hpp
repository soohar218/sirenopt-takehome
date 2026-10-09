#pragma once

#include "sirenopt/batch_queue.hpp"

#include <exception>
#include <memory>
#include <thread>

namespace sirenopt {

class SensorAcquisition {
public:
    explicit SensorAcquisition(std::unique_ptr<ISensor> sensor,
                               std::size_t capacity_batches = 64);
    ~SensorAcquisition();

    SensorAcquisition(const SensorAcquisition&) = delete;
    SensorAcquisition& operator=(const SensorAcquisition&) = delete;

    void start();
    void request_stop() noexcept;
    void join();
    void stop();

    SensorId id() const noexcept { return sensor_->id(); }
    BoundedBatchQueue& queue() noexcept { return queue_; }
    SensorStats& stats() noexcept { return stats_; }
    const SensorStats& stats() const noexcept { return stats_; }

private:
    std::unique_ptr<ISensor> sensor_;
    SensorStats stats_;
    BoundedBatchQueue queue_;
    std::atomic<bool> stop_requested_{false};
    std::thread worker_;
    std::exception_ptr worker_error_;
    bool started_{false};
};

}  // namespace sirenopt
