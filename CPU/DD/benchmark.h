#ifndef BENCHMARK_H
#define BENCHMARK_H

#include <vector>

#include "MMap.h"

#include "Ideal.h"
#include "Merge.h"
#include "Ours.h"

#define THREAD_NUM 16

template<typename Key>
class Benchmark{
public:
    typedef std::unordered_map<uint64_t, int32_t> HashMap;

    Benchmark(std::string PATH){
	    result = Load(PATH.c_str());

        uint32_t length = result.length  / sizeof(Key);
        Key* dataset = (Key*)result.start;

        std::vector<Key> arr(dataset, dataset + length);
        std::sort(arr.begin(), arr.end());

        for(uint32_t i = 0; i < 1000;++i){
            mp[i] = arr[(uint32_t)(i * 0.001 * length)];
        }
    }

    ~Benchmark(){
    	UnLoad(result);
    }

    void Bench(){
        std::vector<Abstract*> absVec = {
#ifdef ACCURACY
                new DD_Ideal<Key>(),
#endif
                new DD_Merge<Key, THREAD_NUM>(),
                new DD_Ours<Key, THREAD_NUM>(),
                };

        for(auto alg : absVec){
            alg->update(result.start, result.length, mp);
            delete alg;
        }
    }

private:
    LoadResult result;
    HashMap mp;
};

#endif
