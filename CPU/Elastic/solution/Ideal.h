#ifndef IDEAL_H
#define IDEAL_H

#include "Abstract.h"

template<typename Key>
class Ideal : public Abstract{
public:

    struct Bucket{
        int32_t vote;
        uint8_t flags[COUNTER_PER_BUCKET];
        Key ID[COUNTER_PER_BUCKET];
        int32_t count[COUNTER_PER_BUCKET];
    };

    void update(void* start, uint32_t size, HashMap mp){
        uint32_t length = size  / sizeof(Key);
        Key* dataset = (Key*)start;
        const uint32_t sketch_length = HASH_NUM * LENGTH, bucket_length = HEAP_SIZE * 3 / COUNTER_PER_BUCKET;

        int32_t* sketch = new int32_t [sketch_length];
        memset(sketch, 0, sizeof(int32_t) * sketch_length);

        Bucket* buckets = new Bucket [bucket_length];
        memset(buckets, 0, sizeof(Bucket) * bucket_length);

        for(uint32_t i = 0;i < length;++i){
            uint32_t pos = hash(dataset[i]) % bucket_length, minPos = 0;
            int32_t minVal = 0x7fffffff;

            for (uint32_t j = 0; j < COUNTER_PER_BUCKET; j++){
                if(buckets[pos].ID[j] == dataset[i]) {
                    buckets[pos].count[j] += 1;
                    goto Ideal_End;
                }

                if(buckets[pos].count[j] == 0){
                    buckets[pos].ID[j] = dataset[i];
                    buckets[pos].count[j] = 1;
                    goto Ideal_End;
                }

                if(buckets[pos].count[j] < minVal){
                    minPos = j;
                    minVal = buckets[pos].count[j];
                }
            }

            if((buckets[pos].vote + 1) >= minVal * LAMBDA){
                buckets[pos].vote = 0;
                buckets[pos].flags[minPos] = 1;

                uint32_t position = hash(buckets[pos].ID[minPos], 101) % sketch_length;
                sketch[position] = sketch[position] + buckets[pos].count[minPos];

                buckets[pos].ID[minPos] = dataset[i];
                buckets[pos].count[minPos] = 1;
            }
            else {
                buckets[pos].vote += 1;
                uint32_t position = hash(dataset[i], 101) % sketch_length;
                sketch[position] = sketch[position] + 1;
            }

            Ideal_End:
            {};
        }

        std::cout << "Ideal Accuracy:" << std::endl;
        HashMap ret;
        for(uint32_t i = 0;i < HEAP_SIZE * 3 / COUNTER_PER_BUCKET;++i){
            for(uint32_t j = 0;j < COUNTER_PER_BUCKET;++j){
                if(buckets[i].flags[j] == 1){
                    ret[buckets[i].ID[j]] = buckets[i].count[j] +
                                            sketch[hash(buckets[i].ID[j], 101) % (HASH_NUM * LENGTH)];
                }
                else{
                    ret[buckets[i].ID[j]] = buckets[i].count[j];
                }
            }
        }

        HHCompare(ret, mp, size / sizeof(Key) * ALPHA);


        delete [] sketch;
        delete [] buckets;
    }
};

#endif
