CPU Code
============

Repository structure
--------------------
*  `CM/`: Codes related to the *Count-Min sketch*
*  `Coco/`: Codes related to the *CocoSketch*
*  `Count/`: Codes related to the *Count sketch*
*  `DD/`: Codes related to the *DDSketch*
*  `Elastic/`: Codes related to the *Elastic sketch*
*  `HLL/`: Codes related to the *HyperLogLog*
*  `LL/`: Codes related to the *LogLog*
*  `Locher/`: Codes related to the *Locher Sketch*
*  `UnivMon/`: Codes related to the *UnivMon*
*  `template/`: Common codes for different sketches

Structure in each folder of sketches
--------------------
*  `config.h`: the parameters for sketches and experiments
*  `benchmark.h`: the benchmarks about accuracy and throughput
*  `Ideal.h`: the codes to calculate the *ideal accuracy*
*  `Merge.h`: the codes of *periodically merging*
*  `Ours.h`: the codes of *OctoSketch*

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