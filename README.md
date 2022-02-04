# Spillway: High-Performance and Real-Time Network Monitoring over Multiple Cores

## Introduction

**Spillway** is a software monitoring framework that can support a spectrum of existing sketches to achieve high online accuracy and high throughput over multiple cores. In contrast to prior work that periodically merges sketches from each core to obtain the aggregated result, we adopt a continuous, push-based mechanism for sketches to perform the aggregation. This design ensures the high accuracy of the aggregated result at any query time and reduces computation costs to achieve high throughput. We apply Spillway to six popular sketches in CPU and DPDK. Our results demonstrate that Spillway achieves around 15.5× lower errors and up to 3.4× higher throughput than the state-of-the-art.

## About this repo

- `CPU`: Spillway for sketches implemented on CPU
- `DPDK`: Spillway for sketches implemented on DPDK
- more details can be found in folders.