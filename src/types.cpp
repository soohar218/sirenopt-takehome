#include "sirenopt/types.hpp"

namespace sirenopt {

SensorStatsSnapshot SensorStats::snapshot() const noexcept {
    return {
        received.load(std::memory_order_relaxed),
        dropped.load(std::memory_order_relaxed),
        rejected.load(std::memory_order_relaxed),
        gaps.load(std::memory_order_relaxed),
        duplicates.load(std::memory_order_relaxed),
        late.load(std::memory_order_relaxed),
    };
}

}  // namespace sirenopt
