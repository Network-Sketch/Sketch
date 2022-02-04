#ifndef OURS_H
#define OURS_H

#include <random>
#include "Abstract.h"


template<uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef Heap<uint32_t, int32_t> myHeap;

    struct Entry{
        uint32_t src;
        uint32_t pos;
        uint32_t value;

        Entry(uint32_t _src = 0, uint32_t _pos = 0, uint32_t _value = 0):
                src(_src), pos(_pos), value(_value){};
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

        double distinct[HASH_NUM] = {0};
        HLL* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new HLL [LENGTH];
            memset(sketch[i], HARDTHRES, sizeof(HLL) * LENGTH);
        }
        auto heap = new myHeap(HEAP_SIZE);

        std::thread thd[thread_num];
        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(ChildThread, &(thd[i]), i, start, size, &(que[i]), &finish, sketch);
        }

        collect(sketch, heap, que, finish, distinct);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Ours Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), (*mp));
#endif
        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete heap;
    }

    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
                            myQueue* que, SIGNAL* finish, HLL** coreSketch){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        HLL* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new HLL [LENGTH];
            memset(sketch[i], HARDTHRES, sizeof(HLL) * LENGTH);
        }

        std::vector<Packet> dataset;

        Partition<thread_num>((Packet*)start, size / sizeof(Packet), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        for(uint32_t i = 0;i < THP_TIME;++i)
            insert(dataset, sketch, *que, coreSketch);
        end = now();
        std::cout << "Ours Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, *que, coreSketch);
#endif

        (*finish) += 1;
        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
    }

    inline static void insert(const std::vector<Packet>& dataset, HLL** sketch, myQueue& q, HLL** coreSketch){
        uint32_t start, finish;
        uint32_t length = dataset.size();

        for(uint32_t i = 0;i < length;++i){
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i].src, hashPos) % LENGTH;
                uint32_t temp = hash(dataset[i].dst, hashPos);
                uint32_t inbucket_index = (temp & 0x1);
                uint32_t bucket_index = ((temp >> 1) & 0x7);
                uint8_t rank = MIN(Upper, __builtin_clz(temp) + 1);

                Buckets& tempBucket = sketch[hashPos][pos].buckets[bucket_index];
                Buckets coreBucket = coreSketch[hashPos][pos].buckets[bucket_index];

                switch(inbucket_index){
                    case 0:
                        if(tempBucket.counter0 < rank){
                            if(coreBucket.counter0 < rank){
                                tempBucket.counter0 = rank;
                                q.enqueue(Entry(dataset[i].src, hashPos * LENGTH + pos, temp));
                            }
                            else{
                                tempBucket.counter0 = coreBucket.counter0;
                            }
                        }
                        break;
                    case 1:
                        if(tempBucket.counter1 < rank){
                            if(coreBucket.counter1 < rank){
                                tempBucket.counter1 = rank;
                                q.enqueue(Entry(dataset[i].src, hashPos * LENGTH + pos, temp));
                            }
                            else{
                                tempBucket.counter1 = coreBucket.counter1;
                            }
                        }
                        break;
                }
            }
        }

    }

    inline static void collect(HLL** sketch, myHeap* heap,
                               myQueue* que, SIGNAL& finish, double* distinct){
        Entry temp;
        bool start = false;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;

                    double before = sketch[hashPos][pos].Query();

                    uint32_t inbucket_index = (temp.value & 0x1);
                    uint32_t bucket_index = ((temp.value >> 1) & 0x7);
                    uint8_t rank = MIN(Upper, __builtin_clz(temp.value) + 1);
                    bool modify = false;

                    Buckets& tempBucket = sketch[hashPos][pos].buckets[bucket_index];
                    switch(inbucket_index){
                        case 0:
                            if(tempBucket.counter0 < rank){
                                tempBucket.counter0 = rank;
                                modify = true;
                                break;
                            }
                        case 1:
                            if(tempBucket.counter1 < rank){
                                tempBucket.counter1 = rank;
                                modify = true;
                                break;
                            }
                    }

                    if(modify){
                        double after = sketch[hashPos][pos].Query();
                        distinct[hashPos] += (after - before);
                        after = after - (distinct[hashPos] - after) / (LENGTH - 1);

                        if(after > heap->min()){
                            double estimation[HASH_NUM] = {0};

                            for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                                uint32_t tempPos = hash(temp.src, tempHash) % LENGTH;
                                double est = sketch[tempHash][tempPos].Query();
                                estimation[tempHash] = est - (distinct[tempHash] - est) / (LENGTH - 1);
                            }
                            heap->Insert(temp.src, MEDIAN3(estimation));
                        }
                    }
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;

                double before = sketch[hashPos][pos].Query();

                uint32_t inbucket_index = (temp.value & 0x1);
                uint32_t bucket_index = ((temp.value >> 1) & 0x7);
                uint8_t rank = MIN(Upper, __builtin_clz(temp.value) + 1);
                bool modify = false;

                Buckets& tempBucket = sketch[hashPos][pos].buckets[bucket_index];
                switch(inbucket_index){
                    case 0:
                        if(tempBucket.counter0 < rank){
                            tempBucket.counter0 = rank;
                            modify = true;
                            break;
                        }
                    case 1:
                        if(tempBucket.counter1 < rank){
                            tempBucket.counter1 = rank;
                            modify = true;
                            break;
                        }
                }

                if(modify){
                    double after = sketch[hashPos][pos].Query();
                    distinct[hashPos] += (after - before);
                    after = after - (distinct[hashPos] - after) / (LENGTH - 1);

                    if(after > heap->min()){
                        double estimation[HASH_NUM] = {0};
                        estimation[hashPos] = after;

                        for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                            if(tempHash != hashPos){
                                uint32_t tempPos = hash(temp.src, tempHash) % LENGTH;
                                double est = sketch[tempHash][tempPos].Query();
                                estimation[tempHash] = est - (distinct[tempHash] - est) / (LENGTH - 1);
                            }
                        }
                        heap->Insert(temp.src, MEDIAN3(estimation));
                    }
                }
            }
        }
    }

};

#endif
