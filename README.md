# SirenOpt Multi-Sensor Data Acquisition

A C++17 acquisition pipeline that simulates three sensors with different
interfaces and rates and merges their readings into one host-timestamped stream.
Sensor B has a complete UDP acquisition path; Sensors A and C use mocks behind
the same sensor interface.

## Requirements

- Linux with a C++17-compatible compiler and CMake 3.16 or newer.
- No third-party runtime libraries. The application uses POSIX UDP sockets and
  the C++ standard library.

## Build and Run

```sh
cmake -S . -B build
cmake --build build
./build/sirenopt
```

The application starts the three acquisition threads, Sensor B's localhost
mock sender, the merge thread, and a counting consumer. Press **Ctrl+C** to
stop producers, drain queued and buffered samples, and print final statistics.
The default merge holdback is 10 ms (configurable with --holdback-ms, e.g., ./build/sirenopt --holdback-ms 15).

## Run Tests

```sh
ctest --test-dir build --output-on-failure
```

The five CTest targets cover packet serialization and validation, Sensor B's
sequence tracking and localhost UDP behavior, bounded queue accounting, merge
ordering and failures, and SIGINT shutdown of the runnable application. All
five passed in the Linux verification run. An optional 60-second benchmark
uses the same mock settings and prints per-sensor throughput, lag percentiles,
queue-depth samples, drops, CPU use, and final accounting:

```sh
./build/stability_benchmark 60
```

The benchmark is not a CTest target; results depend on the host and workload.

## System Overview

| Sensor | Interface | Nominal rate | Reading |
| --- | --- | ---: | --- |
| A | Simulated analog callback | 100 kHz | 1,000-sample blocks |
| B | UDP receiver and mock sender | 1 kHz | Sequence, device timestamp, value |
| C | Simulated serial line | 10 Hz | `T=23.41,P=1013.2` |

Each sensor has an independent acquisition thread and a bounded FIFO queue.
The merge thread polls A, B, then C, taking up to four complete batches per
sensor per round. It orders buffered samples by host observation timestamp,
then sensor ID and original per-sensor order for buffered ties. Equal
timestamps remain valid even after an earlier tie has been emitted.

## Sensor B

UDP Packet Format: Each packet is 24 bytes, containing a 2-byte SB magic header, 1-byte version, 1-byte reserved field, 4-byte sequence number, 8-byte device timestamp (nanoseconds), and 8-byte IEEE-754 value. Multibyte fields use big-endian byte order. Malformed packets and non-finite values are rejected.

The output mode is selected through `SensorBConfig`:

- **ArrivalOrder (default):** Forward valid packets immediately without waiting for missing sequence numbers.
  Duplicates and packets considered late by the sequence tracker are rejected.
- **SequenceOrder:** Buffer future packets and emit received packets in sequence
  order when earlier packets arrive or their gaps are finalized.

Both modes track duplicates, out-of-order arrivals, recovered holes, and
finalized gaps. Missing sequences are finalized when either the tracking window 
is exceeded or the gap timeout expires, whichever occurs first. At the nominal 
1 kHz rate, the 16-packet window may limit reordering tolerance before the 
20 ms timeout is reached. The timeout is checked during idle UDP receive polling. 
Tracking begins with the first valid packet, so earlier loss cannot be detected.
Sequence arithmetic supports 32-bit wraparound within the window. The mock
sender can inject periodic loss, reordering, duplicates, and malformed packets.

## Timestamps and Buffering

- Host observation time uses `std::chrono::steady_clock` and is meaningful
  only within one process run. A timestamps all samples at callback arrival;
  B timestamps immediately after UDP receive; C timestamps a complete line.
  Device and simulated acquisition times remain separate metadata and are not
  used to claim synchronized physical measurement times.
- Each per-sensor queue holds up to 64 **batches** by default, not 64 samples.
  Submission uses `try_lock` and never waits for downstream progress. A full,
  closed, or briefly contended queue drops the incoming batch; a rejected
  1,000-sample A block adds 1,000 to that sensor's `dropped` count. Consumer
  work runs outside the queue lock.
- The merge buffer is bounded in samples. Its configurable 10 ms default
  holdback makes a sample eligible once its host timestamp is at least 10 ms
  old; actual processing lag may be higher. A sample older than the last
  emitted host timestamp is counted and dropped as merge-late. Merge-buffer
  overflow is counted separately.

## Runtime Statistics

The consumer counts emitted samples and measures processing lag. The main
thread prints an interval report approximately once per second. For example:

```text
interval | emitted A= 98000 B=   977 C=     9 | lag ms avg= 13.76 last= 10.40 | drops=0
         | events B[gap=13 reject=7 dup=4]
```

`emitted` is the number delivered per sensor in that interval; `lag` is the
interval average and most recent host-observation-to-consumer delay. `drops`
counts valid samples lost at queues or merge. The optional `events` line shows
nonzero interval gaps, rejections, duplicates, late packets, and merge drops.

On shutdown, the final table reports cumulative counts in **samples**:

- `received`: validated readings before queue admission.
- `emitted`: readings successfully delivered to the consumer.
- `dropped`: valid readings lost at queue submission or in the merge stage.
- `rejected`: malformed, duplicate, or late input readings excluded before
  queue admission.

The report also includes Sensor B sequence statistics and merge-specific
late, overflow, and uncertain-delivery counters. On a successful graceful
shutdown with no consumer failure, it verifies
`valid received = emitted + dropped` for each sensor; rejected inputs are
separate and are not included in `received`.

## Limits and Design Rationale

The 64-batch queues, 20 ms B gap timeout, and 10 ms merge holdback are
configurable starting points, not universal optima. In five short macOS runs
per holdback setting with the baseline mocks and 4/4/4 polling, 10 ms reduced
observed B merge-late drops to 0-2 per run with a median average processing
lag of 13.44 ms; 15 ms eliminated observed drops but increased median lag to
16.24 ms. A longer instrumented macOS run and a separate Linux application
run both maintained final accounting, but these measurements do not establish
10x-rate performance or guarantee the same latency on another host.

Graceful shutdown stops and joins producers before joining the merge thread,
which drains their closed queues and flushes held samples. Destroying a
running merge stage instead is an emergency abort: source queues may retain
unaccounted samples. If the consumer throws, merging stops and `join()`
propagates the error; the attempted delivery is counted as uncertain and is
not retried.

See the [PDF design note](<Multi-Sensor Data Acquisition Design Note.pdf>) for
threading, timestamp alignment, sensor failure handling, 10x bottlenecks, and
the Sensor B implementation rationale.

## AI Usage

I used OpenAI's ChatGPT and Codex for code generation, debugging, tests,
architecture discussions, benchmarks, and documentation. I reviewed and
iteratively refined the results, using tests and measured behavior to evaluate
polling and holdback trade-offs rather than accepting suggestions without
validation.
