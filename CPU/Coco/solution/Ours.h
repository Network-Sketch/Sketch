#ifndef OURS_H
#define OURS_H

#include <random>
#include "Abstract.h"


template<typename Key, uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef uint16_t Value;

    struct Entry{
        Key key;
        uint16_t pos[2];
        Value value;
        bool pos_valid;

        Entry(Key _key = 0, uint16_t _pos0 = 0, uint16_t _pos1 = 0,
                Value _value = 0, uint16_t _pos_valid = true):
                key(_key), value(_value), pos_valid(_pos_valid){
            pos[0] = _pos0;
            pos[1] = _pos1;
        };

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

        Key* keys[HASH_NUM];
        int32_t* counters[HASH_NUM];

        for(uint32_t i = 0;i < HASH_NUM;++i){
            keys[i] = new Key [LENGTH];
            memset(keys[i], 0, sizeof(Key) * LENGTH);

            counters[i] = new int32_t [LENGTH];
            memset(counters[i], 0, sizeof(int32_t) * LENGTH);
        }

        std::thread thd[thread_num];
        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(ChildThread, &(thd[i]), i, start, size, &(que[i]), &finish);
        }

        collect(keys, counters, que, finish);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Ours Accuracy:" << std::endl;

        HashMap ret;

        for(uint32_t i = 0;i < HASH_NUM;++i){
            for(uint32_t j = 0;j < LENGTH;++j){
                ret[keys[i][j]] = counters[i][j];
            }
        }

        HHCompare(ret, (*mp), size / sizeof(Key) * ALPHA);
#endif

        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] keys[i];
            delete [] counters[i];
        }
    }

    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
                            myQueue* que, SIGNAL* finish){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        Key* keys[HASH_NUM];
        Value* counters[HASH_NUM];

        std::random_device rd;
        std::mt19937 rng(rd());

        for(uint32_t i = 0;i < HASH_NUM;++i){
            keys[i] = new Key [LENGTH];
            memset(keys[i], 0, sizeof(Key) * LENGTH);

            counters[i] = new Value [LENGTH];
            memset(counters[i], 0, sizeof(Value) * LENGTH);
        }

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        for(uint32_t i = 0;i < THP_TIME;++i)
            insert(dataset, keys, counters, *que, rng);
        end = now();
        std::cout << "Ours Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, keys, counters, *que, rng);
#endif

        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] keys[i];
            delete [] counters[i];
        }
        (*finish) += 1;
    }

    inline static void insert(const std::vector<Key>& dataset, Key** keys, Value** counters, myQueue& q, std::mt19937& rng){
        uint32_t start, finish;
        uint32_t length = dataset.size();
        uint32_t choice;
        uint16_t pos[HASH_NUM];

        for(uint32_t i = 0;i < length;++i){
            *((uint32_t*)pos) = hash(dataset[i], 0);

            if(keys[0][pos[0]] == dataset[i]){
                counters[0][pos[0]] += 1;
                if(counters[0][pos[0]] >= PROMASK){
                    q.enqueue(Entry(dataset[i], pos[0], pos[1], counters[0][pos[0]]));
                    counters[0][pos[0]] = 0;
                }
                goto OurEnd;
            }

            if(keys[1][pos[1]] == dataset[i]){
                counters[1][pos[1]] += 1;
                if(counters[1][pos[1]] >= PROMASK){
                    q.enqueue(Entry(dataset[i], pos[0], pos[1], counters[1][pos[1]]));
                    counters[1][pos[1]] = 0;
                }
                goto OurEnd;
            }

            choice = (counters[0][pos[0]] > counters[1][pos[1]]);
            counters[choice][pos[choice]] += 1;
            if(rng() % counters[choice][pos[choice]] == 0){
                keys[choice][pos[choice]] = dataset[i];
                if(counters[choice][pos[choice]] >= PROMASK){
                    q.enqueue(Entry(keys[choice][pos[choice]], pos[0], pos[1], counters[choice][pos[choice]]));
                    counters[choice][pos[choice]] = 0;
                }
            }
            else{
                if(counters[choice][pos[choice]] >= PROMASK){
                    q.enqueue(Entry(keys[choice][pos[choice]], pos[0], pos[1], counters[choice][pos[choice]], false));
                    counters[choice][pos[choice]] = 0;
                }
            }

            OurEnd:
            {}
        }

    }

    inline static void collect(Key** keys, int32_t** counters,
                               myQueue* que, SIGNAL& finish){
        Entry temp;
        std::random_device rd;
        std::mt19937 rng(rd());

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    if(!temp.pos_valid){
                        *((uint32_t*)temp.pos) = hash(temp.key, 0);
                    }

                    if(keys[0][temp.pos[0]] == temp.key){
                        counters[0][temp.pos[0]] += temp.value;
                        continue;
                    }

                    if(keys[1][temp.pos[1]] == temp.key){
                        counters[1][temp.pos[1]] += temp.value;
                        continue;
                    }

                    uint32_t choice = (counters[0][temp.pos[0]] > counters[1][temp.pos[1]]);
                    counters[choice][temp.pos[choice]] += temp.value;
                    if(rng() % counters[choice][temp.pos[choice]] < temp.value){
                        keys[choice][temp.pos[choice]] = temp.key;
                    }
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){

                if(keys[0][temp.pos[0]] == temp.key){
                    counters[0][temp.pos[0]] += temp.value;
                    continue;
                }

                if(keys[1][temp.pos[1]] == temp.key){
                    counters[1][temp.pos[1]] += temp.value;
                    continue;
                }

                uint32_t choice = (counters[0][temp.pos[0]] > counters[1][temp.pos[1]]);
                counters[choice][temp.pos[choice]] += temp.value;
                if(rng() % counters[choice][temp.pos[choice]] < temp.value){
                    keys[choice][temp.pos[choice]] = temp.key;
                }
            }
        }
    }

};

#endif
