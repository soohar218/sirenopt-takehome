#include "sirenopt/sensor_b.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

namespace sirenopt {
namespace {

static_assert(sizeof(double) == sizeof(std::uint64_t), "64-bit double required");
static_assert(std::numeric_limits<double>::is_iec559, "IEEE-754 required");

void put_be(std::uint8_t* out, std::uint64_t value, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
        out[count - i - 1] = static_cast<std::uint8_t>(value);
        value >>= 8;
    }
}

std::uint64_t get_be(const std::uint8_t* in, std::size_t count) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < count; ++i) {
        value = (value << 8) | in[i];
    }
    return value;
}

class SocketFd {
public:
    explicit SocketFd(int fd) : fd_(fd) {
        if (fd_ < 0) throw std::runtime_error("UDP socket creation failed");
    }
    ~SocketFd() { ::close(fd_); }
    SocketFd(const SocketFd&) = delete;
    SocketFd& operator=(const SocketFd&) = delete;
    int get() const noexcept { return fd_; }

private:
    int fd_;
};

sockaddr_in loopback_address(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}

void submit_ready(ReorderResult& result, ISampleSink& sink,
                  SensorStats& stats) {
    stats.gaps.fetch_add(result.gaps, std::memory_order_relaxed);
    for (auto& sample : result.ready) {
        SampleBatch batch;
        batch.push_back(std::move(sample));
        if (!sink.try_submit(std::move(batch))) {
            stats.dropped.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace

BPacketBytes encode_b_packet(const BPacket& packet) noexcept {
    BPacketBytes bytes{};
    bytes[0] = 'S';
    bytes[1] = 'B';
    bytes[2] = 1;
    put_be(bytes.data() + 4, packet.sequence, 4);
    put_be(bytes.data() + 8, packet.device_timestamp_ns, 8);
    std::uint64_t bits;
    std::memcpy(&bits, &packet.value, sizeof(bits));
    put_be(bytes.data() + 16, bits, 8);
    return bytes;
}

std::optional<BPacket> decode_b_packet(const std::uint8_t* data,
                                       std::size_t size) noexcept {
    if (size != kBPacketSize || data == nullptr || data[0] != 'S' ||
        data[1] != 'B' || data[2] != 1 || data[3] != 0) {
        return std::nullopt;
    }
    BPacket packet{};
    packet.sequence = static_cast<std::uint32_t>(get_be(data + 4, 4));
    packet.device_timestamp_ns = get_be(data + 8, 8);
    const auto bits = get_be(data + 16, 8);
    std::memcpy(&packet.value, &bits, sizeof(bits));
    if (!std::isfinite(packet.value)) return std::nullopt;
    return packet;
}

BReorderBuffer::BReorderBuffer(std::size_t window) : window_(window) {
    if (window == 0 || window > 65536) {
        throw std::invalid_argument("reorder window must be 1..65536");
    }
}

void BReorderBuffer::remember(std::uint32_t sequence) {
    recent_order_.push_back(sequence);
    recent_set_.insert(sequence);
    if (recent_order_.size() > window_) {
        recent_set_.erase(recent_order_.front());
        recent_order_.pop_front();
    }
}

void BReorderBuffer::release_contiguous(ReorderResult& result) {
    while (true) {
        auto it = pending_.find(*expected_);
        if (it == pending_.end()) break;
        result.ready.push_back(std::move(it->second));
        remember(*expected_);
        pending_.erase(it);
        ++*expected_;
    }
}

ReorderResult BReorderBuffer::ingest(const BPacket& packet,
                                    HostTime arrival) {
    ReorderResult result;
    if (!expected_) expected_ = packet.sequence;

    const std::uint32_t distance = packet.sequence - *expected_;
    if (distance >= 0x80000000u) {
        result.disposition = recent_set_.count(packet.sequence)
                                 ? PacketDisposition::duplicate
                                 : PacketDisposition::late;
        return result;
    }
    if (pending_.count(packet.sequence)) {
        result.disposition = PacketDisposition::duplicate;
        return result;
    }

    while (static_cast<std::uint32_t>(packet.sequence - *expected_) >=
           window_) {
        const auto before = *expected_;
        release_contiguous(result);
        if (*expected_ == before) {
            if (pending_.empty()) {
                const auto distance_now =
                    static_cast<std::uint32_t>(packet.sequence - *expected_);
                const auto skipped = distance_now -
                                     static_cast<std::uint32_t>(window_ - 1);
                result.gaps += skipped;
                *expected_ += skipped;
            } else {
                ++result.gaps;
                ++*expected_;
            }
        }
    }

    pending_.emplace(packet.sequence,
                     Sample{SensorId::B, arrival, packet.device_timestamp_ns,
                            NetworkReading{packet.value, packet.sequence}});
    release_contiguous(result);
    return result;
}

ReorderResult BReorderBuffer::flush() {
    ReorderResult result;
    while (!pending_.empty()) {
        auto next = pending_.begin();
        for (auto it = pending_.begin(); it != pending_.end(); ++it) {
            if (static_cast<std::uint32_t>(it->first - *expected_) <
                static_cast<std::uint32_t>(next->first - *expected_)) {
                next = it;
            }
        }
        result.gaps += static_cast<std::uint32_t>(next->first - *expected_);
        *expected_ = next->first;
        release_contiguous(result);
    }
    return result;
}

SensorB::SensorB(SensorBConfig config) : config_(config) {
    if (config_.reorder_window == 0 || config_.reorder_window > 65536) {
        throw std::invalid_argument("reorder window must be 1..65536");
    }
}

std::uint16_t SensorB::bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
}

void SensorB::run(ISampleSink& sink, const std::atomic<bool>& stop_requested,
                  SensorStats& stats) {
    SocketFd socket(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto bind_address = loopback_address(config_.port);
    if (::bind(socket.get(), reinterpret_cast<const sockaddr*>(&bind_address),
               sizeof(bind_address)) != 0) {
        throw std::runtime_error(std::string("Sensor B UDP bind failed: ") +
                                 std::strerror(errno));
    }
    sockaddr_in actual_address{};
    socklen_t address_size = sizeof(actual_address);
    if (::getsockname(socket.get(),
                      reinterpret_cast<sockaddr*>(&actual_address),
                      &address_size) != 0) {
        throw std::runtime_error("Sensor B getsockname failed");
    }
    const timeval timeout{0, 20000};
    if (::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                     sizeof(timeout)) != 0) {
        throw std::runtime_error("Sensor B receive timeout setup failed");
    }
    bound_port_.store(ntohs(actual_address.sin_port),
                      std::memory_order_release);

    BReorderBuffer reorder(config_.reorder_window);
    while (!stop_requested.load(std::memory_order_relaxed)) {
        std::uint8_t bytes[256];
        const auto size = ::recvfrom(socket.get(), bytes, sizeof(bytes), 0,
                                     nullptr, nullptr);
        const auto arrival = std::chrono::steady_clock::now();
        if (size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
            throw std::runtime_error("Sensor B UDP receive failed");
        }
        const auto packet = decode_b_packet(bytes, static_cast<std::size_t>(size));
        if (!packet) {
            stats.rejected.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        auto result = reorder.ingest(*packet, arrival);
        if (result.disposition != PacketDisposition::accepted) {
            stats.rejected.fetch_add(1, std::memory_order_relaxed);
            if (result.disposition == PacketDisposition::duplicate)
                stats.duplicates.fetch_add(1, std::memory_order_relaxed);
            else
                stats.late.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        stats.received.fetch_add(1, std::memory_order_relaxed);
        submit_ready(result, sink, stats);
    }
    auto remaining = reorder.flush();
    submit_ready(remaining, sink, stats);
    bound_port_.store(0, std::memory_order_release);
}

MockBSender::MockBSender(MockBSenderConfig config) : config_(config) {
    if (config_.port == 0) {
        throw std::invalid_argument("mock sender needs receiver port");
    }
}

void MockBSender::run(const std::atomic<bool>& stop_requested) const {
    SocketFd socket(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto destination = loopback_address(config_.port);
    const auto transmit = [&](const BPacketBytes& bytes) {
        const auto sent = ::sendto(socket.get(), bytes.data(), bytes.size(), 0,
                                   reinterpret_cast<const sockaddr*>(&destination),
                                   sizeof(destination));
        if (sent != static_cast<ssize_t>(bytes.size())) {
            throw std::runtime_error("mock sender UDP send failed");
        }
    };

    std::optional<BPacketBytes> held;
    auto next_send = std::chrono::steady_clock::now();
    for (std::uint32_t sequence = 0;
         !stop_requested.load(std::memory_order_relaxed) &&
         (config_.packet_limit == 0 || sequence < config_.packet_limit);
         ++sequence) {
        const auto ordinal = sequence + 1;
        const bool lost = config_.loss_every &&
                          ordinal % config_.loss_every == 0;
        if (!lost) {
            auto bytes = encode_b_packet(
                {sequence, static_cast<std::uint64_t>(sequence) *
                               config_.interval_us * 1000,
                 static_cast<double>(sequence) + 0.5});
            if (config_.malformed_every &&
                ordinal % config_.malformed_every == 0) {
                bytes[2] = 0xff;
            }
            if (held) {
                transmit(bytes);
                transmit(*held);
                held.reset();
            } else if (config_.reorder_every &&
                       ordinal % config_.reorder_every == 0) {
                held = bytes;
            } else {
                transmit(bytes);
            }
            if (config_.duplicate_every &&
                ordinal % config_.duplicate_every == 0 && !held) {
                transmit(bytes);
            }
        }
        next_send += std::chrono::microseconds(config_.interval_us);
        std::this_thread::sleep_until(next_send);
    }
    if (held) transmit(*held);
}

}  // namespace sirenopt
