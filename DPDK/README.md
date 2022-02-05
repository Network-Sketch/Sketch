DPDK Code
============

Implementation
--------------------
We integrated Spillway with [DPDK] (version 22.03.0). Each Spillway worker is integrated with the polling mode thread in DPDK. Our testbed has two servers. Each server is equipped with a Mellanox ConnectX5 Ex 100G NIC. One server generates high-speed TCP
traffic using [pktgen-dpdk] (version 21.11.0), while another server runs DPDK to receive packets and process them using Spillway.

Repository structure
--------------------
*  `common/`: the common functions and data structures we use (e.g., hash, heap)
*  `queue/`: the shared [queue] we use 
*  `Sketch_Merge.h`: *Merging* for the Sketch
*  `Sketch_Ours.h`: *Spillway* for the Sketch

[DPDK]: https://www.dpdk.org/
[pktgen-dpdk]: https://github.com/pktgen/Pktgen-DPDK
[queue]: https://github.com/cameron314/readerwriterqueue