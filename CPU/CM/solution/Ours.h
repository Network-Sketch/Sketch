#ifndef OURS_H
#define OURS_H

#include <random>
#include "Abstract.h"


template<typename Key, uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef Heap<Key, int32_t> myHeap;
    typedef uint16_t Value;

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
        uint32_t pos[HASH_NUM] = {0};

        for(uint32_t i = 0;i < length;++i){
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                pos[hashPos] = hash(dataset[i], hashPos) % LENGTH;
            }

            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                sketch[hashPos][pos[hashPos]] += 1;
                if(sketch[hashPos][pos[hashPos]] >= PROMASK){
                    q.enqueue(Entry(dataset[i], hashPos * LENGTH + pos[hashPos], sketch[hashPos][pos[hashPos]]));
                    sketch[hashPos][pos[hashPos]] = 0;
                }
            }
        }
    }

    inline static void collect(int32_t** sketch, myHeap* heap,
                               myQueue* que, SIGNAL& finish){
        Entry temp;
        int32_t number = 0, length = 0;
        int32_t old_thres = -1, old_send = -1;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;
                    sketch[hashPos][pos] += temp.value;
                    number += 1;

                    if(sketch[hashPos][pos] > heap->min()){
                        int32_t minimum = sketch[hashPos][pos];

                        for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                            uint32_t tempPos = hash(temp.key, tempHash) % LENGTH;
                            minimum = MIN(minimum, sketch[tempHash][tempPos]);
                        }
                        heap->Insert(temp.key, minimum);
                    }

                    /*
                    if(number == 100){
                        int32_t new_length = 0;
                        for(uint32_t j = 0;j < thread_num;++j){
                            new_length += que[j].size_approx();
                        }

                        //printf("Queue: %d %d\n", new_length, length);

                        int32_t new_send = number + new_length - length;
                        int32_t expected_send = MAX(0, number + 100 - new_length);

                        //printf("Send: %d %d %d\n", expected_send, new_send, old_send);

                        if(old_send == -1 || new_send == old_send || PROMASK == old_thres){
                            int32_t new_thres;

                            if(expected_send == 0)
                                new_thres = (PROMASK << 1);
                            else
                                new_thres = new_send * PROMASK / expected_send;

                            new_thres = MIN(1024, MIN(PROMASK << 1, new_thres));
                            PROMASK = MAX(0x2, MAX(PROMASK >> 1, new_thres));
                        }
                        else{
                            int32_t delta = (expected_send - new_send) *
                                            (PROMASK - old_thres) / (new_send - old_send);
                            delta -= (delta == 0);
                            int32_t new_thres = PROMASK + delta;

                            new_thres = MIN(1024, MIN(PROMASK << 1, new_thres));
                            PROMASK = MAX(0x2, MAX(PROMASK >> 1, new_thres));
                        }

                        old_send = new_send;
                        old_thres = PROMASK;

                        number = 0;
                        length = new_length;

                        printf("%d %d\n", int(PROMASK), length);
                    }
                    */

                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                uint32_t hashPos = temp.pos / LENGTH, pos = temp.pos % LENGTH;
                sketch[hashPos][pos] += temp.value;

                if(sketch[hashPos][pos] > heap->min()){
                    int32_t minimum = sketch[hashPos][pos];

                    for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                        uint32_t tempPos = hash(temp.key, tempHash) % LENGTH;
                        minimum = MIN(minimum, sketch[tempHash][tempPos]);
                    }
                    heap->Insert(temp.key, minimum);
                }
            }
        }
    }

};

#endif
