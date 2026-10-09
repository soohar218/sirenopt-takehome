# SirenOpt Multi-Sensor Data Acquisition

A C++17 acquisition pipeline that collects data from three simulated sensors and merges their readings into a single host-timestamped, time-ordered stream. Sensor B has a complete UDP acquisition path; Sensors A and C use mocks.

## Build and Run

Requires Linux, a C++17-compatible compiler, and CMake 3.16+. No third-party runtime libraries are required.

```bash
cmake -S . -B build
cmake --build build
./build/sirenopt
```

Press **Ctrl+C** for graceful shutdown and final statistics. The default merge holdback is 10 ms and can be overridden with `--holdback-ms`, e.g., `./build/sirenopt --holdback-ms 15`.

## Tests

Run automated tests:

```bash
ctest --test-dir build --output-on-failure
```

Tests cover UDP packet handling, sequence tracking, bounded queues, merge ordering, and shutdown behavior.

For a longer stability and performance check:

```bash
./build/stability_benchmark 60
```

This runs a 60-second workload and reports throughput, latency percentiles, queue occupancy, drops, and final accounting. It is optional and not part of CTest.

## System Overview

| Sensor | Interface | Rate | Reading |
|---|---|---|---|
| A | Analog mock | 100 kHz | 1,000-sample callback blocks |
| B | UDP receiver + mock sender | 1 kHz | Sequence, device timestamp, value |
| C | Serial mock | 10 Hz | ASCII line (`T=23.41,P=1013.2`) |

Each sensor runs in its own acquisition thread with a bounded FIFO queue. A dedicated merge thread polls the queues and delivers samples in nondecreasing host-timestamp order.

## Sensor B

**UDP Packet Format:** Each packet is 24 bytes: `SB` magic (2), version (1), reserved field (1), sequence number (4), device timestamp in nanoseconds (8), and IEEE-754 value (8). Multibyte fields use big-endian byte order. Malformed packets and non-finite values are rejected.

Two forwarding modes are supported:

- **ArrivalOrder (default):** Forwards valid packets immediately, rejecting duplicates and packets considered late by sequence tracking.
- **SequenceOrder:** Buffers packets to reconstruct sequence order, finalizing missing gaps as needed.

Both modes track loss, duplicates, and reordering. The default sequence window is 16 packets with a 20 ms gap timeout. Missing sequences are finalized when either limit is reached first. These are configurable engineering defaults, not measured optima.

## Assumptions and Limitations

- **Timestamps:** The host's `steady_clock` is the common reference. Samples are timestamped at host observation, not physical measurement time, because sensor clocks are unsynchronized.
- **Buffering:** Each sensor queue holds up to 64 batches by default. Submission uses nonblocking `try_lock`; incoming batches are dropped and counted if the queue is full, closed, or contended.
- **Ordering:** The merge uses a configurable 10 ms holdback. Samples older than the last emitted timestamp are dropped; equal timestamps are allowed. Actual processing lag may exceed 10 ms.
- **Performance:** The implementation targets the specified baseline rates; sustained 10× throughput is not guaranteed.

See the [PDF design note](<Multi-Sensor Data Acquisition Design Note.pdf>) for
threading, timestamp alignment, sensor failure handling, 10x bottlenecks, and
the Sensor B implementation rationale.

## AI Usage

I used OpenAI's ChatGPT and Codex for code generation, debugging, tests,
architecture discussions, benchmarks, and documentation. I reviewed and
iteratively refined the results, using tests and measured behavior to evaluate
polling and holdback trade-offs rather than accepting suggestions without
validation.
