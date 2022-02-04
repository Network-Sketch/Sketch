#ifndef IDEAL_H
#define IDEAL_H

#include "Abstract.h"

template<typename Key>
class Ideal : public Abstract{
public:

    void update(void* start, uint32_t size, HashMap mp){
        uint32_t length = size  / sizeof(Key);
        Key* dataset = (Key*)start;

        Key* keys[HASH_NUM];
        int32_t* counters[HASH_NUM];

        std::random_device rd;
        std::mt19937 rng(rd());

        for(uint32_t i = 0;i < HASH_NUM;++i){
            keys[i] = new Key [LENGTH];
            memset(keys[i], 0, sizeof(Key) * LENGTH);

            counters[i] = new int32_t [LENGTH];
            memset(counters[i], 0, sizeof(int32_t) * LENGTH);
        }

        for(uint32_t i = 0;i < length;++i){
            int32_t minimum = 0x7fffffff;
            uint32_t minPos, minHash;
            bool replace = true;

            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i], hashPos) % LENGTH;

                if(keys[hashPos][pos] == dataset[i]){
                    counters[hashPos][pos] += 1;
                    replace = false;
                    break;
                }
                if(counters[hashPos][pos] < minimum){
                    minPos = pos;
                    minHash = hashPos;
                    minimum = counters[hashPos][pos];
                }
            }

            if(replace){
                counters[minHash][minPos] += 1;
                if(rng() % counters[minHash][minPos] == 0){
                    keys[minHash][minPos] = dataset[i];
                }
            }
        }

        std::cout << "Ideal Accuracy:" << std::endl;

        HashMap ret;

        for(uint32_t i = 0;i < HASH_NUM;++i){
            for(uint32_t j = 0;j < LENGTH;++j){
                ret[keys[i][j]] = counters[i][j];
            }
        }

        HHCompare(ret, mp, length * ALPHA);


        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] keys[i];
            delete [] counters[i];
        }
    }
};

#endif
