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

        int32_t* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new int32_t [LENGTH];
            memset(sketch[i], 0, sizeof(int32_t) * LENGTH);
        }
        myHeap* heap = new myHeap(HEAP_SIZE);

        for(uint32_t i = 0;i < length;++i){
            int32_t number[HASH_NUM] = {0};
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t hashNum = hash(dataset[i], hashPos);
                uint32_t pos = (hashNum >> 1) % LENGTH;
                int32_t incre = increment[hashNum & 1];

                sketch[hashPos][pos] += incre;
                number[hashPos] = sketch[hashPos][pos] * incre;
            }
            heap->Insert(dataset[i], MEDIAN3(number));
        }

        std::cout << "Ideal Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), mp, length * ALPHA);


        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete heap;
    }
};

#endif
