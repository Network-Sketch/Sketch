CPU Code
============

Repository structure
--------------------
*  `CM/`: Codes related to the *Count-Min sketch*
*  `Count/`: Codes related to the *Count sketch*
*  `HLL/`: Codes related to the *Locher Sketch*
*  `UnivMon/`: Codes related to the *UnivMon*
*  `Elastic/`: Codes related to the *Elastic sketch*
*  `Coco/`: Codes related to the *CocoSketch*

Structure in each folder
--------------------
*  `common/`: the common functions and data structures we use (e.g., mmap, heap)
*  `queue/`: the shared [queue] we use 
*  `benchmark.h`: the benchmarks about accuracy and throughput
*  `solution/Ideal.h`: the codes to calculate the *ideal accuracy*
*  `solution/Merge.h`: the codes of *periodically merging*
*  `solution/Ours.h`: the codes of *Spillway*

Requirements
-------
- cmake
- g++

How to run
-------
```bash
$ cd CM
$ cmake .
$ make
$ ./CM your-dataset
```

[queue]: https://github.com/cameron314/readerwriterqueue