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
    typedef std::unordered_map<Key, int32_t> HashMap;

    Benchmark(std::string PATH){
	    result = Load(PATH.c_str());

        uint32_t length = result.length  / sizeof(Key);
        Key* dataset = (Key*)result.start;

        for(uint32_t i = 0;i < length;++i){
            mp[dataset[i]] += 1;
        }
    }

    ~Benchmark(){
    	UnLoad(result);
    }

    void Bench(){
        std::vector<Abstract*> absVec = {
#ifdef ACCURACY
                new Elastic_Ideal<Key>(),
#endif
                new Elastic_Merge<Key, THREAD_NUM>(),
                new Elastic_Ours<Key, THREAD_NUM>(),
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
