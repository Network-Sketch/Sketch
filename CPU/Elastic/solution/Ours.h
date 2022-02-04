#ifndef OURS_H
#define OURS_H

#include "Abstract.h"

template<typename Key, uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef uint16_t Value;

    struct Bucket{
        int32_t vote;
        Key ID[COUNTER_PER_BUCKET];
        int32_t count[COUNTER_PER_BUCKET];
    };

    struct Entry{
        Key key;
        uint32_t pos;
        Value value;
        bool bucket;

        Entry(Key _key = 0, uint32_t _pos = 0, Value _value = 0, bool _bucket = false):
                key(_key), pos(_pos), value(_value), bucket(_bucket){};
    };

    typedef ReaderWriterQueue<Entry> myQueue;

    void update(void* start, uint32_t size, HashMap mp){
        std::thread parent = std::thread(ParentThread, &parent, start, size, &mp);
        parent.join();
    }

    static void ParentThread(std::thread* thisThd, void* start, uint32_t size, HashMap* mp){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_num))
            return;
#endif
        SIGNAL finish(0);

        myQueue que[thread_num];

        int32_t* sketch = new int32_t [HASH_NUM * LENGTH];
        memset(sketch, 0, sizeof(int32_t) * HASH_NUM * LENGTH);

        Bucket* buckets = new Bucket [HEAP_SIZE * 3 / COUNTER_PER_BUCKET];
        memset(buckets, 0, sizeof(Bucket) * (HEAP_SIZE * 3 / COUNTER_PER_BUCKET));

        std::thread thd[thread_num];
        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(ChildThread, &(thd[i]), i, start, size, &(que[i]), &finish);
        }

        collect(sketch, buckets, que, finish);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Ours Accuracy:" << std::endl;
        HashMap ret;
        for(uint32_t i = 0;i < HEAP_SIZE * 3 / COUNTER_PER_BUCKET;++i){
            for(uint32_t j = 0;j < COUNTER_PER_BUCKET;++j){
                ret[buckets[i].ID[j]] = buckets[i].count[j] +
                                        sketch[hash(buckets[i].ID[j], 101) % (HASH_NUM * LENGTH)];
            }
        }

        HHCompare(ret, (*mp), size / sizeof(Key) * ALPHA);
#endif

        delete [] sketch;
        delete [] buckets;
    }


    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
                            myQueue* que, SIGNAL* finish)
    {
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        Value* sketch = new Value [HASH_NUM * LENGTH];
        memset(sketch, 0, sizeof(Value) * HASH_NUM * LENGTH);

        Bucket* buckets = new Bucket [HEAP_SIZE * 3 / COUNTER_PER_BUCKET];
        memset(buckets, 0, sizeof(Bucket) * (HEAP_SIZE * 3 / COUNTER_PER_BUCKET));

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        for(uint32_t i = 0;i < THP_TIME;++i)
            insert(dataset, sketch, buckets, *que);
        end = now();
        std::cout << "Ours Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, buckets, *que);
#endif

        (*finish) += 1;

        delete [] sketch;
        delete [] buckets;
    }


    inline static void insert(const std::vector<Key>& dataset, Value* sketch, Bucket* buckets, myQueue& q){
        uint32_t length = dataset.size();
        const uint32_t sketch_length = HASH_NUM * LENGTH, bucket_length = HEAP_SIZE * 3 / COUNTER_PER_BUCKET;

        for(uint32_t i = 0;i < length;++i){
            uint32_t pos = hash(dataset[i]) % bucket_length, minPos = 0;
            int32_t minVal = 0x7fffffff;

            for (uint32_t j = 0; j < COUNTER_PER_BUCKET; j++){
                if(buckets[pos].ID[j] == dataset[i]) {
                    buckets[pos].count[j] += 1;
                    if(buckets[pos].count[j] >= PROMASK){
                        q.enqueue(Entry(buckets[pos].ID[j], pos, buckets[pos].count[j], true));
                        buckets[pos].count[j] = 0;
                    }
                    goto Ours_Child_End;
                }

                if(buckets[pos].count[j] < minVal){
                    minPos = j;
                    minVal = buckets[pos].count[j];
                }
            }

            if((buckets[pos].vote + 1) >= minVal * LAMBDA){
                buckets[pos].vote = 0;

                if(minVal != 0){
                    uint32_t position = hash(buckets[pos].ID[minPos], 101) % sketch_length;
                    sketch[position] = sketch[position] + buckets[pos].count[minPos];
                    if(sketch[position] >= PROMASK){
                        q.enqueue(Entry(buckets[pos].ID[minPos], position, sketch[position], false));
                        sketch[position] = 0;
                    }
                }

                buckets[pos].ID[minPos] = dataset[i];
                buckets[pos].count[minPos] = 1;
            }
            else {
                buckets[pos].vote += 1;
                uint32_t position = hash(dataset[i], 101) % sketch_length;
                sketch[position] = sketch[position] + 1;
                if(sketch[position] >= PROMASK){
                    q.enqueue(Entry(dataset[i], position, sketch[position], false));
                    sketch[position] = 0;
                }
            }

            Ours_Child_End:
            {};
        }
    }

    inline static void collect(int32_t* sketch, Bucket* bucket, myQueue* que, SIGNAL& finish){
        Entry temp;
        const uint32_t sketch_length = HASH_NUM * LENGTH, bucket_length = HEAP_SIZE * 3 / COUNTER_PER_BUCKET;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    if(temp.bucket){
                        int32_t minVal = 0x7fffffff;
                        uint32_t minPos = 0;

                        for(uint32_t j = 0; j < COUNTER_PER_BUCKET; j++){
                            if(bucket[temp.pos].ID[j] == temp.key) {
                                bucket[temp.pos].count[j] += temp.value;
                                goto Ours_Collect_End0;
                            }

                            if(bucket[temp.pos].count[j] < minVal){
                                minPos = j;
                                minVal = bucket[temp.pos].count[j];
                            }
                        }

                        if((bucket[temp.pos].vote + temp.value) >= minVal * LAMBDA){
                            bucket[temp.pos].vote = 0;

                            if(minVal != 0){
                                uint32_t position = hash(bucket[temp.pos].ID[minPos], 101) % sketch_length;
                                sketch[position] = sketch[position] + bucket[temp.pos].count[minPos];
                            }

                            bucket[temp.pos].ID[minPos] = temp.key;
                            bucket[temp.pos].count[minPos] = temp.value;
                        }
                        else {
                            bucket[temp.pos].vote += temp.value;
                            uint32_t position = hash(temp.key, 101) % sketch_length;
                            sketch[position] = sketch[position] + temp.value;
                        }

                    }
                    else{
                        sketch[temp.pos] += temp.value;
                    }

                    Ours_Collect_End0:
                    {};
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                if(temp.bucket){
                    int32_t minVal = 0x7fffffff;
                    uint32_t minPos = 0;

                    for(uint32_t j = 0; j < COUNTER_PER_BUCKET; j++){
                        if(bucket[temp.pos].ID[j] == temp.key) {
                            bucket[temp.pos].count[j] += temp.value;
                            goto Ours_Collect_End1;
                        }

                        if(bucket[temp.pos].count[j] < minVal){
                            minPos = j;
                            minVal = bucket[temp.pos].count[j];
                        }
                    }

                    if((bucket[temp.pos].vote + temp.value) >= minVal * LAMBDA){
                        bucket[temp.pos].vote = 0;

                        if(minVal != 0){
                            uint32_t position = hash(bucket[temp.pos].ID[minPos], 101) % sketch_length;
                            sketch[position] = sketch[position] + bucket[temp.pos].count[minPos];
                        }

                        bucket[temp.pos].ID[minPos] = temp.key;
                        bucket[temp.pos].count[minPos] = temp.value;
                    }
                    else {
                        bucket[temp.pos].vote += temp.value;
                        uint32_t position = hash(temp.key, 101) % sketch_length;
                        sketch[position] = sketch[position] + temp.value;
                    }

                }
                else{
                    sketch[temp.pos] += temp.value;
                }

                Ours_Collect_End1:
                {};
            }
        }
    }
};


#endif
