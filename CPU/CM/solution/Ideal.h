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
            int32_t minimum = 0x7fffffff;
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i], hashPos) % LENGTH;
                sketch[hashPos][pos] += 1;
                minimum = MIN(minimum, sketch[hashPos][pos]);
            }
            heap->Insert(dataset[i], minimum);
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
