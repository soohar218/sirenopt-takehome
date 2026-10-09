# Design Note

### 1. Threading and Buffering

The system follows a **producer–consumer architecture**, with three independent acquisition threads (one per sensor) and a dedicated merge thread. Each sensor implements a common acquisition interface and submits valid readings to its own bounded, thread-safe FIFO queue. Queue capacity is measured in batches, with a default of 64 batches per sensor. Sensor A submits blocks of 1,000 samples, while Sensors B and C submit individual readings.

Queue submission is nonblocking using `try_lock`. If the queue is full, its lock is unavailable, or the queue is closed, the incoming batch is dropped and counted. This **drop-newest policy** ensures that acquisition threads never wait for downstream processing, while keeping memory usage bounded. The 64-batch capacity is a configurable starting point for absorbing temporary bursts rather than an optimized value.

A single merge thread polls the queues in A → B → C order, processing up to four complete batches per sensor per round. Samples are placed in a bounded, timestamp-ordered buffer and emitted after a configurable **10 ms holdback** to accommodate queueing and scheduling delays. Samples with timestamps earlier than the last emitted timestamp are dropped as merge-late, while equal timestamps remain valid.

The consumer continuously processes emitted samples, and the main thread reports statistics approximately once per second. During graceful shutdown, acquisition threads stop and join before the merge thread drains remaining data and flushes its buffer for final accounting.

### 2. Timestamp Alignment

Each sensor has its own clock, and the host clock is the reference. Only Sensor B carries an explicit device timestamp in its input packet. Sensor A reports batches through a callback, while Sensor C provides ASCII readings without timestamps. The mocks preserve simulated timing metadata for A and C, but that metadata does not establish a shared measurement-time timeline across the three sensors.

Although Sensor B's device clock could theoretically serve as a reference, doing so would require a reliable mapping between B's clock and the host clock, as well as measurement timestamps or timing models for Sensors A and C. UDP arrival timestamps alone cannot establish that mapping accurately because they include variable network delay.

We therefore use the host's monotonic clock as the common reference and assign timestamps when each sensor's data is observed by the host: at callback arrival for A, after UDP reception for B, and when a complete ASCII line is received for C. Samples are merged in host-observation-time order, while available device timing metadata is preserved separately. This provides consistent ordering without implying synchronization of the underlying physical measurements.

### 3. Sensor Failure Handling

Each sensor has different failure characteristics. Recoverable input errors are handled within that sensor's acquisition path, so a malformed reading or packet does not stop the other sensors.

**Sensor A — Delayed callbacks and bursts.** Sensor A delivers 1,000-sample blocks, and occasional callback delays can cause multiple blocks to arrive close together. Its bounded queue absorbs temporary bursts, while nonblocking submission prevents acquisition from waiting on the merge thread. If the queue cannot accept a block, the entire block is dropped and its samples are counted. Callback observation timestamps are kept separate from simulated acquisition times so that delayed delivery is not mistaken for delayed measurement.

**Sensor B — Packet loss, reordering, and corruption.** UDP packets are validated before processing, and their sequence numbers are tracked to detect duplicates, missing packets, and out-of-order arrivals. A bounded sequence-tracking window distinguishes temporarily missing packets from finalized gaps and can recognize packets that recover an earlier gap. In the default arrival-order mode, valid unique packets are forwarded without waiting for missing sequence numbers, avoiding additional latency. Device timestamps and sequence numbers are preserved as metadata.

**Sensor C — Corrupted serial readings.** Sensor C parses ASCII readings containing temperature and pressure values. Malformed or corrupted lines are rejected and counted, while subsequent valid readings continue to be processed normally.

**Shared failure handling.** All three sensors use bounded queues and sample-counted drops when submission fails. The merge stage separately counts and drops samples that arrive earlier than the last emitted host timestamp. The final report distinguishes validated readings received, samples emitted or dropped, and invalid or duplicate readings rejected.

### 4. Bottlenecks at 10× Rates

At 10× the specified rates, Sensor A would generate 1,000,000 samples/s, Sensor B 10,000 packets/s, and Sensor C 100 readings/s. Sensor A would account for approximately 99% of the total.

The **primary scaling concern is the merge stage**. A fixed holdback requires buffering samples that arrive during that interval, so a 10× input rate substantially increases the capacity needed even if processing keeps up. The single merge thread also inserts samples individually into a timestamp-ordered container. Buffer capacity, allocation, and per-sample ordering work could each limit throughput, causing drops or growing queues.

**Queue capacity and scheduling latency are secondary concerns.** In baseline experiments, polling several large A batches before B increased B's receive-to-merge delay. Polling fewer A batches reduced that delay but caused substantial A merge-late drops. We retained the 4/4/4 polling policy and selected the 10 ms holdback based on the observed latency–loss trade-off.

At 10× rates, I would first measure merge throughput, buffer occupancy, per-sensor queue depth, polling delay, CPU use, drops, and tail latency under sustained load. Depending on the results, improvements might include sizing the merge buffer for the measured holdback workload, reducing per-sample allocation and ordering overhead, or revisiting polling policy. These are expected bottlenecks, not verified 10× performance limits; the current implementation has only been benchmarked at the specified baseline rates.

### 5. Why Sensor B Was Chosen

I chose to fully implement **Sensor B (UDP)** because it presents a combination of networking, data integrity, and timing challenges within a manageable scope. Unlike Sensor A's block-based acquisition or Sensor C's relatively simple serial parsing, Sensor B requires handling packet loss, duplication, reordering, and independent device timestamps.

Implementing Sensor B end-to-end allowed me to explore the distinction between **packet sequence order and host-timestamp order**. I initially implemented sequence-number-based reordering to emit received packets in sequence order, finalizing gaps when missing packets do not arrive. However, because the merged stream uses host timestamps as a common reference across all sensors, waiting for missing sequence numbers could introduce unnecessary buffering and latency. I therefore added an **arrival-order mode** that forwards valid, unique packets immediately while continuing to track sequence gaps, duplicates, and out-of-order arrivals.

I selected arrival-order forwarding as the default while retaining sequence-order mode as an alternative. This allows the merge stage to apply a consistent host-time ordering policy across sensors while preserving device timestamps and sequence numbers for data integrity and downstream analysis.

This approach demonstrates practical trade-offs between correctness, latency, and reliability while keeping the implementation focused on the core acquisition and merging requirements.
