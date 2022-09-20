XDP Code
============

Implementation
--------------------
We integrated OctoSketch with XDP in Linux kernel 5.15.0 using SKB mode. Our testbed has two servers that are the same as the DPDK implementation.

Structure in each folder of sketches
--------------------
*  `compile.sh`: the bash script to compile files
*  `Ours-main.c`: the codes of *aggregator* in the user space
*  `Ours-XDP.c`: the codes of *workers* in the kernel space

How to run
-------
```bash
$ cd CM
$ sudo ./compile
$ sudo ./main
```

