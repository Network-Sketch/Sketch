#ifndef BENCHMARK_H
#define BENCHMARK_H


#include <vector>

#include "MMap.h"

#include "Ideal.h"
#include "Merge.h"
#include "Ours.h"

#define THREAD_NUM 16

class Benchmark{
public:

    typedef std::unordered_map<uint32_t, int32_t> HashMap;

    Benchmark(std::string PATH){
	    result = Load(PATH.c_str());

        uint32_t length = result.length  / sizeof(Packet);
        Packet* dataset = (Packet*)result.start;

        std::unordered_set<uint64_t> st;
        for(uint32_t i = 0;i < length;++i){
            if(st.find(dataset[i]) == st.end()){
                st.insert(dataset[i]);
                mp[dataset[i].src] += 1;
            }
        }
        std::cout << st.size() << std::endl;
    }

    ~Benchmark(){
    	UnLoad(result);
    }

    void Bench(){
        std::vector<Abstract*> absVec = {
#ifdef ACCURACY
                new Ideal(),
#endif
                new Merge<THREAD_NUM>(),
                new Ours<THREAD_NUM>(),
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
