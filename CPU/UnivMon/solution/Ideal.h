#ifndef IDEAL_H
#define IDEAL_H

#include "Abstract.h"

template<typename Key>
class Ideal : public Abstract{
public:

    typedef Heap<Key, int32_t> myHeap;

    void update(void* start, uint32_t size, HashMap mp){

        uint32_t length = size  / sizeof(Key);
        Key* dataset = (Key*)start;

        int32_t* sketch[MAX_LEVEL][HASH_NUM];
        myHeap* heap[MAX_LEVEL];

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                sketch[i][j] = new int32_t [LENGTH];
                memset(sketch[i][j], 0, sizeof(int32_t) * LENGTH);
            }
            heap[i] = new myHeap(HEAP_SIZE);
        }

        for(uint32_t i = 0;i < length;++i){
            uint32_t polar = hash(dataset[i], 199);
            uint32_t max_level = MIN(MAX_LEVEL - 1, __builtin_clz(polar));

            for(uint32_t level = 0; level <= max_level;++level){
                int32_t number[HASH_NUM] = {0};
                for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                    uint32_t hashNum = hash(dataset[i], level * HASH_NUM + hashPos);
                    uint32_t pos = (hashNum >> 1) % LENGTH;
                    int32_t incre = increment[hashNum & 1];

                    sketch[level][hashPos][pos] += incre;
                    number[hashPos] = sketch[level][hashPos][pos] * incre;
                }
                heap[level]->Insert(dataset[i], MEDIAN3(number));
            }
        }

        std::cout << "Ideal Accuracy:" << std::endl;

        HashMap ret;

        for(int32_t level = 0;level < MAX_LEVEL;++level){
            HashMap temp = heap[level]->AllQuery();
            for(auto it = temp.begin();it != temp.end();++it){
                if(ret.find(it->first) == ret.end()){
                    ret[it->first] = it->second;
                }
            }
        }

        HHCompare(ret, mp, length * ALPHA);

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                delete [] sketch[i][j];
            }
            delete heap[i];
        }
    }
};

#endif
