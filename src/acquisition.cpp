#include "sirenopt/acquisition.hpp"

#include <stdexcept>

namespace sirenopt {

SensorAcquisition::SensorAcquisition(std::unique_ptr<ISensor> sensor,
                                     std::size_t capacity_batches)
    : sensor_(std::move(sensor)), queue_(capacity_batches, stats_) {
    if (!sensor_) throw std::invalid_argument("sensor must not be null");
}

SensorAcquisition::~SensorAcquisition() {
    request_stop();
    if (worker_.joinable()) worker_.join();
    queue_.close();
}

void SensorAcquisition::start() {
    if (started_ || stop_requested_.load(std::memory_order_relaxed)) {
        throw std::logic_error("acquisition can start only once");
    }
    worker_ = std::thread([this] {
        try {
            sensor_->run(queue_, stop_requested_, stats_);
        } catch (...) {
            worker_error_ = std::current_exception();
        }
        queue_.close();
    });
    started_ = true;
}

void SensorAcquisition::request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_relaxed);
}

void SensorAcquisition::join() {
    if (worker_.joinable()) worker_.join();
    queue_.close();
    if (worker_error_) std::rethrow_exception(worker_error_);
}

void SensorAcquisition::stop() {
    request_stop();
    join();
}

}  // namespace sirenopt
