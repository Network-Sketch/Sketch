#ifndef IDEAL_H
#define IDEAL_H

#include "Abstract.h"

class Ideal : public Abstract{
public:

    typedef Heap<uint32_t, int32_t> myHeap;

    void update(void* start, uint32_t size, HashMap mp){
        uint32_t length = size  / sizeof(Packet);
        Packet* dataset = (Packet*)start;

        double distinct[HASH_NUM] = {0};
        HLL* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new HLL [LENGTH];
            memset(sketch[i], 0, sizeof(HLL) * LENGTH);
        }
        myHeap* heap = new myHeap(HEAP_SIZE);

        for(uint32_t i = 0;i < length;++i){
            double estimation[HASH_NUM] = {0};
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i].src, hashPos) % LENGTH;
                distinct[hashPos] -= sketch[hashPos][pos].Query();
                sketch[hashPos][pos].Insert(dataset[i].dst, hashPos);
                double est = sketch[hashPos][pos].Query();
                estimation[hashPos] = est - distinct[hashPos] / (LENGTH - 1);
                distinct[hashPos] += est;
            }
            heap->Insert(dataset[i].src, MEDIAN3(estimation));
        }

        std::cout << "Ideal Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), mp);


        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete heap;
    }
};

#endif
