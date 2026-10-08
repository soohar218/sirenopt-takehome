#include "sirenopt/batch_queue.hpp"

#include <stdexcept>
#include <utility>

namespace sirenopt {

BoundedBatchQueue::BoundedBatchQueue(std::size_t capacity_batches,
                                     SensorStats& stats)
    : capacity_batches_(capacity_batches), stats_(stats),
      slots_(capacity_batches) {
    if (capacity_batches_ == 0) {
        throw std::invalid_argument("queue capacity must be positive");
    }
}

bool BoundedBatchQueue::try_submit(SampleBatch&& batch) noexcept {
    const auto sample_count = batch.size();
    std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        stats_.dropped.fetch_add(sample_count, std::memory_order_relaxed);
        return false;
    }
    if (closed_ || size_ == capacity_batches_) {
        stats_.dropped.fetch_add(sample_count, std::memory_order_relaxed);
        return false;
    }
    slots_[(head_ + size_) % capacity_batches_].emplace(std::move(batch));
    ++size_;
    lock.unlock();
    available_.notify_one();
    return true;
}

std::optional<SampleBatch> BoundedBatchQueue::try_pop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (size_ == 0) return std::nullopt;
    SampleBatch batch = std::move(*slots_[head_]);
    slots_[head_].reset();
    head_ = (head_ + 1) % capacity_batches_;
    --size_;
    return batch;
}

std::optional<SampleBatch> BoundedBatchQueue::wait_pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    available_.wait(lock, [this] { return closed_ || size_ != 0; });
    if (size_ == 0) return std::nullopt;
    SampleBatch batch = std::move(*slots_[head_]);
    slots_[head_].reset();
    head_ = (head_ + 1) % capacity_batches_;
    --size_;
    return batch;
}

void BoundedBatchQueue::close() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
    }
    available_.notify_all();
}

std::size_t BoundedBatchQueue::size_batches() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
}

}  // namespace sirenopt
