DPDK Code
============

Implementation
--------------------
We integrated OctoSketch with [DPDK] (version 21.11). Each OctoSketch worker is integrated with the polling mode thread in DPDK. Our testbed has two servers. Each server is equipped with a Mellanox ConnectX5 Ex 100G NIC. One server generates high-speed TCP traffic using [pktgen-dpdk], while another server runs DPDK to receive packets and process them using OctoSketch.

Structure in each folder of sketches
--------------------
*  `config.h`: the parameters for sketches and experiments
*  `Merge.h`: the codes of *periodically merging*
*  `Ours.h`: the codes of *OctoSketch*

[DPDK]: https://www.dpdk.org/
[pktgen-dpdk]: https://github.com/pktgen/Pktgen-DPDK