#pragma once

#include "sirenopt/sensor.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <unordered_set>

namespace sirenopt {

struct BPacket {
    std::uint32_t sequence;
    std::uint64_t device_timestamp_ns;
    double value;
};

constexpr std::size_t kBPacketSize = 24;
using BPacketBytes = std::array<std::uint8_t, kBPacketSize>;

BPacketBytes encode_b_packet(const BPacket& packet) noexcept;
std::optional<BPacket> decode_b_packet(const std::uint8_t* data,
                                       std::size_t size) noexcept;

enum class PacketDisposition { accepted, duplicate, late };

struct ReorderResult {
    PacketDisposition disposition{PacketDisposition::accepted};
    SampleBatch ready;
    std::uint64_t gaps{0};
};

class BReorderBuffer {
public:
    explicit BReorderBuffer(std::size_t window);

    ReorderResult ingest(const BPacket& packet, HostTime arrival);
    ReorderResult flush();

private:
    void release_contiguous(ReorderResult& result);
    void remember(std::uint32_t sequence);

    std::size_t window_;
    std::optional<std::uint32_t> expected_;
    std::map<std::uint32_t, Sample> pending_;
    std::deque<std::uint32_t> recent_order_;
    std::unordered_set<std::uint32_t> recent_set_;
};

struct SensorBConfig {
    std::uint16_t port{0};
    std::size_t reorder_window{16};
};

class SensorB final : public ISensor {
public:
    explicit SensorB(SensorBConfig config = {});

    SensorId id() const noexcept override { return SensorId::B; }
    std::uint16_t bound_port() const noexcept;
    void run(ISampleSink& sink, const std::atomic<bool>& stop_requested,
             SensorStats& stats) override;

private:
    SensorBConfig config_;
    std::atomic<std::uint16_t> bound_port_{0};
};

struct MockBSenderConfig {
    std::uint16_t port{0};
    std::uint32_t packet_limit{0};  // Zero means run until stopped.
    std::uint32_t interval_us{1000};
    std::uint32_t loss_every{97};
    std::uint32_t reorder_every{53};
    std::uint32_t duplicate_every{211};
    std::uint32_t malformed_every{307};
};

class MockBSender {
public:
    explicit MockBSender(MockBSenderConfig config);
    void run(const std::atomic<bool>& stop_requested) const;

private:
    MockBSenderConfig config_;
};

}  // namespace sirenopt
