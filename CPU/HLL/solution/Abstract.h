#ifndef ABSTRACT_H
#define ABSTRACT_H

#include "hash.h"
#include "HLL.h"
#include "Heap.h"
#include "readerwriterqueue.h"

#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>

#include <cstring>
#include <pthread.h>

#define HASH_NUM 3
#define LENGTH (1 << 16)
#define HEAP_SIZE 0xff

#define HARDTHRES 0x11

#define THP_TIME 10

#define INTERVAL 100000

#define ACCURACY
//#define THROUGHPUT

#define Partition HashPartition

#define SIGNAL std::atomic_int

class Abstract{
public:
    typedef std::unordered_map<uint32_t, int32_t> HashMap;

    virtual void update(void* start, uint32_t size, HashMap mp) = 0;
    virtual ~Abstract(){};

#ifdef __linux__
    static bool setaffinity(std::thread* thd, uint32_t coreId){
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(coreId, &cpuset);
        int rc = pthread_setaffinity_np(thd->native_handle(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            return false;
        }
        return true;
    }
#endif

    template<uint32_t thread_num>
    inline static void RRPartition(Packet* start, uint32_t size, uint32_t id, std::vector<Packet>& vec){
        for(uint32_t i = id;i < size;i += thread_num){
            vec.push_back(start[i]);
        }
    }

    template<uint32_t thread_num>
    inline static void HashPartition(Packet* start, uint32_t size, uint32_t id, std::vector<Packet>& vec){
        for(uint32_t i = 0;i < size;++i){
            if(hash(start[i].src, 101) % thread_num == id){
                vec.push_back(start[i]);
            }
        }
    }

    static void HHCompare(HashMap test, HashMap real, int32_t threshold = 200){
        double estHH = 0, HH = 0, both = 0;
        double CR = 0, PR = 0, AAE = 0, ARE = 0;

        for(auto it = test.begin();it != test.end();++it){
            if(it->second > threshold){
                estHH += 1;
                if(real[it->first] > threshold){
                    both += 1;
                    AAE += abs(real[it->first] - it->second);
                    ARE += abs(real[it->first] - it->second) / (double)real[it->first];
                }
            }
        }

        for(auto it = real.begin();it != real.end();++it){
            if(it->second > threshold){
                HH += 1;
            }
        }

        //std::cout << estHH << " " << HH << " " << threshold <<  std::endl;

        std::cout << "CR: " << both / HH << std::endl
                  << "PR: " << both / estHH << std::endl
                  << "AAE: " << AAE / both << std::endl
                  << "ARE: " << ARE / both << std::endl << std::endl;
    }
};

double MEDIAN3(double array[3]){
    if(array[0] < array[1]){
        if(array[2] < array[0]){
            return array[0];
        }
        else if(array[2] < array[1]){
            return array[2];
        }
        else{
            return array[1];
        }
    }
    else{
        if(array[2] < array[1]){
            return array[1];
        }
        else if(array[2] < array[0]){
            return array[2];
        }
        else{
            return array[0];
        }
    }
}

#endif
