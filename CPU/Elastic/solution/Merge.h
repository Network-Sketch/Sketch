#ifndef MERGE_H
#define MERGE_H

#include "Abstract.h"

template<typename Key, uint32_t thread_num>
class Merge : public Abstract{
public:

    struct Bucket{
        int32_t vote;
        Key ID[COUNTER_PER_BUCKET];
        int32_t count[COUNTER_PER_BUCKET];
    };

    struct Entry{
        int32_t* sketch;
        Bucket* buckets;

        Entry(int32_t* _sketch = nullptr, Bucket* _buckets = nullptr):
                sketch(_sketch), buckets(_buckets){};
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
        std::cout << "Merge Accuracy:" << std::endl;
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
        int32_t* sketch = new int32_t [HASH_NUM * LENGTH];
        memset(sketch, 0, sizeof(int32_t) * HASH_NUM * LENGTH);

        Bucket* buckets = new Bucket [HEAP_SIZE * 3 / COUNTER_PER_BUCKET];
        memset(buckets, 0, sizeof(Bucket) * (HEAP_SIZE * 3 / COUNTER_PER_BUCKET));

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        insert(dataset, sketch, buckets, *que);
        end = now();
        std::cout << "Merge Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, buckets, *que);
#endif

        (*finish) += 1;
    }


    inline static void insert(const std::vector<Key>& dataset, int32_t* sketch, Bucket* buckets, myQueue& q){
        uint32_t start, finish;
        uint32_t number = 1, length = dataset.size();
        const uint32_t sketch_length = HASH_NUM * LENGTH, bucket_length = HEAP_SIZE * 3 / COUNTER_PER_BUCKET;

#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            uint32_t pos = hash(dataset[i]) % bucket_length, minPos = 0;
            int32_t minVal = 0x7fffffff;

            for (uint32_t j = 0; j < COUNTER_PER_BUCKET; j++){
                if(buckets[pos].ID[j] == dataset[i]) {
                    buckets[pos].count[j] += 1;
                    goto Merge_Child_End;
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
                }

                buckets[pos].ID[minPos] = dataset[i];
                buckets[pos].count[minPos] = 1;
            }
            else {
                buckets[pos].vote += 1;
                uint32_t position = hash(dataset[i], 101) % sketch_length;
                sketch[position] = sketch[position] + 1;
            }

            Merge_Child_End:
            if((number % (INTERVAL * thread_num)) == 0){
                q.enqueue(Entry(sketch, buckets));
                sketch = new int32_t [sketch_length];
                memset(sketch, 0, sizeof(int32_t) * sketch_length);

                buckets = new Bucket [bucket_length];
                memset(buckets, 0, sizeof(Bucket) * bucket_length);
            }
            number += 1;
        }

        delete [] sketch;
        delete [] buckets;
    }

    inline static void collect(int32_t* sketch, Bucket* bucket, myQueue* que, SIGNAL& finish){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                if(que[i].try_dequeue(temp)){
                    collect(sketch, bucket, temp);
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                collect(sketch, bucket, temp);
            }
        }
    }

    inline static void collect(int32_t* sketch, Bucket* buckets, const Entry& temp){
        const uint32_t sketch_length = HASH_NUM * LENGTH, bucket_length = HEAP_SIZE * 3 / COUNTER_PER_BUCKET;

        for(uint32_t i = 0;i < sketch_length;++i){
            sketch[i] += temp.sketch[i];
        }
        delete [] temp.sketch;

        for(uint32_t i = 0;i < bucket_length;++i){
            for(uint32_t j = 0;j < COUNTER_PER_BUCKET;++j){
                int32_t minVal = 0x7fffffff;
                uint32_t minPos = 0;

                for (uint32_t k = 0; k < COUNTER_PER_BUCKET; k++){
                    if(buckets[i].ID[k] == temp.buckets[i].ID[j]){
                        buckets[i].count[k] += temp.buckets[i].count[j];
                        goto Merge_Collect_End;
                    }

                    if(buckets[i].count[k] < minVal){
                        minPos = k;
                        minVal = buckets[i].count[k];
                    }
                }

                if((buckets[i].vote + temp.buckets[i].count[j]) >= minVal * LAMBDA){
                    buckets[i].vote = 0;

                    if(minVal != 0){
                        uint32_t position = hash(buckets[i].ID[minPos], 101) % sketch_length;
                        sketch[position] = sketch[position] + buckets[i].count[minPos];
                    }

                    buckets[i].ID[minPos] = temp.buckets[i].ID[j];
                    buckets[i].count[minPos] = temp.buckets[i].count[j];
                }
                else {
                    buckets[i].vote += temp.buckets[i].count[j];
                    uint32_t position = hash(temp.buckets[i].ID[j], 101) % sketch_length;
                    sketch[position] = sketch[position] + temp.buckets[i].count[j];
                }

                Merge_Collect_End:
                {};
            }
        }

        delete [] temp.buckets;
    }
};

#endif
