#ifndef OURS_H
#define OURS_H

#include <random>
#include "Abstract.h"


template<typename Key, uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef Heap<Key, int32_t> myHeap;
    typedef int16_t Value;

    struct Entry{
        Key key;
        uint32_t pos;
        Value value;

        Entry(Key _key = 0, uint32_t _pos = 0, Value _value = 0):
                key(_key), pos(_pos), value(_value){};
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

        int32_t* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new int32_t [LENGTH];
            memset(sketch[i], 0, sizeof(int32_t) * LENGTH);
        }
        auto heap = new myHeap(HEAP_SIZE);

        std::thread thd[thread_num];
        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(ChildThread, &(thd[i]), i, start, size, &(que[i]), &finish);
        }

        collect(sketch, heap, que, finish);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Ours Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), (*mp), size / sizeof(Key) * ALPHA);
#endif

        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete heap;
    }

    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
                            myQueue* que, SIGNAL* finish){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        Value* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new Value [LENGTH];
            memset(sketch[i], 0, sizeof(Value) * LENGTH);
        }

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        for(uint32_t i = 0;i < THP_TIME;++i)
            insert(dataset, sketch, *que);
        end = now();
        std::cout << "Ours Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, *que);
#endif

        (*finish) += 1;
        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
    }

    inline static void insert(const std::vector<Key>& dataset, Value** sketch, myQueue& q){
        uint32_t start, finish;
        uint32_t length = dataset.size();

        uint32_t hashNum[HASH_NUM] = {0};
        uint32_t pos[HASH_NUM] = {0};
        int32_t incre[HASH_NUM] = {0};
        
        for(uint32_t i = 0;i < length;++i){
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                hashNum[hashPos] = hash(dataset[i], hashPos);
                pos[hashPos] = (hashNum[hashPos] >> 1) % LENGTH;
                incre[hashPos] = increment[hashNum[hashPos] & 1];
            }

            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                sketch[hashPos][pos[hashPos]] += incre[hashPos];
                if(sketch[hashPos][pos[hashPos]] * incre[hashPos] >= PROMASK){
                    q.enqueue(Entry(dataset[i], hashPos * LENGTH + pos[hashPos], sketch[hashPos][pos[hashPos]]));
                    sketch[hashPos][pos[hashPos]] = 0;
                }
            }
        }

    }

    inline static void collect(int32_t** sketch, myHeap* heap,
                               myQueue* que, SIGNAL& finish){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;
                    sketch[hashPos][pos] += temp.value;

                    if(abs(sketch[hashPos][pos]) > heap->min()){
                        int32_t count[HASH_NUM] = {0};

                        for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                            uint32_t hashNum = hash(temp.key, tempHash);
                            uint32_t tempPos = (hashNum >> 1) % LENGTH;
                            int32_t incre = increment[hashNum & 1];

                            count[tempHash] = sketch[tempHash][tempPos] * incre;
                        }
                        heap->Insert(temp.key, MEDIAN3(count));
                    }

                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;
                sketch[hashPos][pos] += temp.value;

                if(abs(sketch[hashPos][pos]) > heap->min()){
                    int32_t count[HASH_NUM] = {0};

                    for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                        uint32_t hashNum = hash(temp.key, tempHash);
                        uint32_t tempPos = (hashNum >> 1) % LENGTH;
                        int32_t incre = increment[hashNum & 1];

                        count[tempHash] = sketch[tempHash][tempPos] * incre;
                    }
                    heap->Insert(temp.key, MEDIAN3(count));
                }

            }
        }
    }

};

#endif
