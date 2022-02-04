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

        int32_t* sketch[MAX_LEVEL][HASH_NUM];
        myHeap* heap[MAX_LEVEL];

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                sketch[i][j] = new int32_t [LENGTH];
                memset(sketch[i][j], 0, sizeof(int32_t) * LENGTH);
            }
            heap[i] = new myHeap(HEAP_SIZE);
        }

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

        HashMap ret;

        for(int32_t level = 0;level < MAX_LEVEL;++level){
            HashMap temp = heap[level]->AllQuery();
            for(auto it = temp.begin();it != temp.end();++it){
                if(ret.find(it->first) == ret.end()){
                    ret[it->first] = it->second;
                }
            }
        }

        HHCompare(ret, (*mp), size / sizeof(Key) * ALPHA);
#endif

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                delete [] sketch[i][j];
            }
            delete heap[i];
        }
    }

    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
                            myQueue* que, SIGNAL* finish){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        Value* sketch[MAX_LEVEL][HASH_NUM];

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                sketch[i][j] = new Value [LENGTH];
                memset(sketch[i][j], 0, sizeof(Value) * LENGTH);
            }
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

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                delete [] sketch[i][j];
            }
        }
        (*finish) += 1;
    }

    inline static void insert(const std::vector<Key>& dataset, Value* sketch[MAX_LEVEL][HASH_NUM], myQueue& q){
        uint32_t start, finish;
        uint32_t length = dataset.size();

        uint32_t hashNum[MAX_LEVEL][HASH_NUM] = {0};
        uint32_t pos[MAX_LEVEL][HASH_NUM] = {0};
        int32_t incre[MAX_LEVEL][HASH_NUM] = {0};

        for(uint32_t i = 0;i < length;++i){
            uint32_t polar = hash(dataset[i], 199);
            uint32_t max_level = MIN(MAX_LEVEL - 1, __builtin_clz(polar));

            for(uint32_t level = 0; level <= max_level;++level){
                for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                    hashNum[level][hashPos] = hash(dataset[i], level * HASH_NUM + hashPos);
                    pos[level][hashPos] = (hashNum[level][hashPos] >> 1) % LENGTH;
                    incre[level][hashPos] = increment[hashNum[level][hashPos] & 1];
                }
            }

            for(uint32_t level = 0; level <= max_level;++level){
                for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                    sketch[level][hashPos][pos[level][hashPos]] += incre[level][hashPos];
                    if(sketch[level][hashPos][pos[level][hashPos]] * incre[level][hashPos] >= PROMASK){
                        q.enqueue(Entry(dataset[i], level * LENGTH * HASH_NUM + hashPos * LENGTH + pos[level][hashPos], sketch[level][hashPos][pos[level][hashPos]]));
                        sketch[level][hashPos][pos[level][hashPos]] = 0;
                    }
                }
            }
        }
    }

    inline static void collect(int32_t* sketch[MAX_LEVEL][HASH_NUM], myHeap* heap[MAX_LEVEL],
                               myQueue* que, SIGNAL& finish){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    uint32_t level = temp.pos / (HASH_NUM * LENGTH), hashNum = temp.pos % (HASH_NUM * LENGTH);
                    uint32_t hashPos = hashNum / LENGTH, pos = hashNum % LENGTH;
                    sketch[level][hashPos][pos] += temp.value;

                    if(abs(sketch[level][hashPos][pos]) > heap[level]->min()){
                        int32_t count[HASH_NUM] = {0};

                        for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                            uint32_t hashNum = hash(temp.key, tempHash);
                            uint32_t tempPos = (hashNum >> 1) % LENGTH;
                            int32_t incre = increment[hashNum & 1];

                            count[tempHash] = sketch[level][tempHash][tempPos] * incre;
                        }
                        heap[level]->Insert(temp.key, MEDIAN3(count));
                    }
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                uint32_t level = temp.pos / (HASH_NUM * LENGTH), hashNum = temp.pos % (HASH_NUM * LENGTH);
                uint32_t hashPos = hashNum / LENGTH, pos = hashNum % LENGTH;
                sketch[level][hashPos][pos] += temp.value;

                if(abs(sketch[level][hashPos][pos]) > heap[level]->min()){
                    int32_t count[HASH_NUM] = {0};

                    for(uint32_t tempHash = 0;tempHash < HASH_NUM;++tempHash){
                        uint32_t hashNum = hash(temp.key, tempHash);
                        uint32_t tempPos = (hashNum >> 1) % LENGTH;
                        int32_t incre = increment[hashNum & 1];

                        count[tempHash] = sketch[level][tempHash][tempPos] * incre;
                    }
                    heap[level]->Insert(temp.key, MEDIAN3(count));
                }

            }
        }
    }

};

#endif
