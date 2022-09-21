# OctoSketch: Enabling Real-Time, Continuous Network Monitoring over Multiple Cores

## Introduction

**OctoSketch** is a software monitoring framework that can scale a wide spectrum of sketches to many cores with high online accuracy and throughput. In contrast to previous systems that adopt straightforward sketch merges from individual cores to obtain the aggregated result, we devise a continuous, change-based mechanism that can be generally applied to sketches to perform the aggregation. This design ensures high online accuracy of the aggregated result at any query time and reduces computation costs to achieve high throughput. We apply OctoSketch to nine representative sketches on three software platforms (CPU, DPDK, and XDP). Our results demonstrate that OctoSketch achieves about 15.6× lower errors and up to 4.5× higher throughput than the state-of-the-art.

Repository structure
--------------------
*  `common/`: the common functions and data structures we use (e.g., hash, heap)
*  `queue/`: the shared [queue] we use 
*  `sketch/`: the sketches we use
*  `CPU/`: OctoSketch for sketches implemented on CPU
*  `DPDK/`: OctoSketch for sketches implemented on DPDK
*  `XDP/`: OctoSketch for sketches implemented on XDP

[queue]: https://github.com/cameron314/readerwriterqueue
