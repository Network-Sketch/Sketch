#ifndef CONFIG_H
#define CONFIG_H

#include "DD.h"

#define LENGTH (1 << 16)

#define INTERVAL 10000

template<typename Key>
using MyDD = DD<Key, LENGTH>;

template<typename Key>
using MyChild_DD = Child_DD<Key, LENGTH>;

#define THP_TIME 200

#define ALPHA 0

#define PROMASK 0xf

#define ACCURACY
//#define THROUGHPUT

#define Partition HashPartition

#endif
