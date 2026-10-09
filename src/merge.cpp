#include "sirenopt/merge.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace sirenopt {

MergeBuffer::MergeBuffer(MergeConfig config) : config_(config) {
    if (config_.holdback.count() < 0 || config_.capacity_samples == 0) {
        throw std::invalid_argument("invalid merge holdback or capacity");
    }
}

MergeStats MergeBuffer::stats() const noexcept {
    return {emitted_.load(std::memory_order_relaxed),
            late_dropped_.load(std::memory_order_relaxed),
            overflow_dropped_.load(std::memory_order_relaxed),
            failed_deliveries_.load(std::memory_order_relaxed)};
}

void MergeBuffer::push(Sample&& sample, SensorStats& source_stats) {
    const auto index = static_cast<std::size_t>(sample.sensor);
    if (index >= next_order_.size()) {
        throw std::invalid_argument("unknown sensor id");
    }
    const Key key{sample.host_timestamp, sample.sensor, next_order_[index]++};
    if (last_emitted_timestamp_ && sample.host_timestamp < *last_emitted_timestamp_) {
        late_dropped_.fetch_add(1, std::memory_order_relaxed);
        source_stats.dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (pending_.size() == config_.capacity_samples) {
        overflow_dropped_.fetch_add(1, std::memory_order_relaxed);
        source_stats.dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    pending_.emplace(key, std::move(sample));
}

void MergeBuffer::emit_first(IMergedConsumer& consumer) {
    auto node = pending_.extract(pending_.begin());
    try {
        consumer.consume(std::move(node.mapped()));
    } catch (...) {
        failed_deliveries_.fetch_add(1, std::memory_order_relaxed);
        throw;
    }
    last_emitted_timestamp_ = node.key().timestamp;
    emitted_.fetch_add(1, std::memory_order_relaxed);
}

void MergeBuffer::emit_ready(HostTime now, IMergedConsumer& consumer) {
    const auto watermark = now - config_.holdback;
    while (!pending_.empty() && pending_.begin()->first.timestamp <= watermark) {
        emit_first(consumer);
    }
}

void MergeBuffer::emit_all(IMergedConsumer& consumer) {
    while (!pending_.empty()) emit_first(consumer);
}

MergeStage::MergeStage(std::array<SensorAcquisition*, 3> sources,
                       IMergedConsumer& consumer, MergeConfig config)
    : sources_(sources), consumer_(consumer), buffer_(config) {
    std::array<bool, 3> seen{};
    for (auto* source : sources_) {
        if (!source) throw std::invalid_argument("merge source is null");
        const auto index = static_cast<std::size_t>(source->id());
        if (index >= seen.size() || seen[index]) {
            throw std::invalid_argument("merge needs one of each sensor");
        }
        seen[index] = true;
    }
    std::sort(sources_.begin(), sources_.end(), [](auto* left, auto* right) {
        return left->id() < right->id();
    });
}

MergeStage::~MergeStage() {
    abort_.store(true, std::memory_order_relaxed);
    if (worker_.joinable()) worker_.join();
}

void MergeStage::start() {
    if (started_) throw std::logic_error("merge can start only once");
    worker_ = std::thread([this] {
        try {
            run();
        } catch (...) {
            worker_error_ = std::current_exception();
        }
    });
    started_ = true;
}

void MergeStage::join() {
    if (worker_.joinable()) worker_.join();
    if (worker_error_) std::rethrow_exception(worker_error_);
}

void MergeStage::run() {
    while (true) {
        bool moved = false;
        for (auto* source : sources_) {
            for (int batch_count = 0; batch_count < 4; ++batch_count) {
                auto batch = source->queue().try_pop();
                if (!batch) break;
                moved = true;
                for (auto& sample : *batch) {
                    buffer_.push(std::move(sample), source->stats());
                }
            }
        }
        buffer_.emit_ready(std::chrono::steady_clock::now(), consumer_);

        const bool all_closed = std::all_of(
            sources_.begin(), sources_.end(), [](auto* source) {
                return source->queue().closed_and_empty();
            });
        if (all_closed || abort_.load(std::memory_order_relaxed)) {
            buffer_.emit_all(consumer_);
            return;
        }
        if (!moved) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace sirenopt
