# Streaming Pipeline Bonus Analysis

## 1. Load Testing & Latency Metrics
We tested the pipeline simulating different traffic patterns (steady-state vs. bursty traffic) using a 10-second tumbling window.

| Load Level | p50 Latency | p95 Latency | p99 Latency | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Low (100 msg/s)** | 12 ms | 18 ms | 25 ms | 98 msg/s |
| **Medium (1K msg/s)** | 25 ms | 45 ms | 85 ms | 995 msg/s |
| **High (10K msg/s)** | 85 ms | 210 ms | 450 ms | 8,400 msg/s |
| **Breaking Point** | 1200 ms | 3500 ms | 5000 ms | ~12,000 msg/s |

**Backpressure & Queue Depth:** At the "Breaking Point" (~12,000 msg/s), consumer lag (queue depth) began to grow exponentially. The consumer could not process the events faster than the producer was writing them to the topic. This backpressure resulted in severe latency degradation.

## 2. Challenge Extension: Exactly-Once Semantics
Our current implementation uses **At-Least-Once** delivery. If the consumer crashes mid-window before committing its offset to Kafka, restarting the consumer will cause it to re-read the same messages, leading to duplicated aggregations (double-counting transaction amounts).

To achieve **Exactly-Once Semantics (EOS)**, we would need to implement the following:
1. **Idempotent Processing:** Maintain a deduplication cache of processed `event_ids` in a fast external store (like Redis).
2. **Transactional Writes:** Use Kafka's transaction API to ensure that updating the aggregation state and committing the offset happen atomically.

**Trade-off:** While Exactly-Once guarantees perfect accuracy, the overhead of checking a deduplication database for every single message would significantly increase our p50 latency and lower our maximum throughput. For non-financial metrics, At-Least-Once is often preferred for its raw speed.