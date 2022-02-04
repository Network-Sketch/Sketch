#ifndef MERGE_H
#define MERGE_H

#include "Abstract.h"

template<typename Key, uint32_t thread_num>
class Merge : public Abstract{
public:

    struct Entry{
        Key** keys;
        int32_t** counters;

        Entry(Key** _keys = nullptr, int32_t** _counters = nullptr):
                keys(_keys), counters(_counters){};
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
        std::cout << "Merge Accuracy:" << std::endl;

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
            myQueue* que, SIGNAL* finish)
            {
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif

        Key** keys;
        int32_t** counters;

        keys = new Key* [HASH_NUM];
        counters = new int32_t* [HASH_NUM];

        std::random_device rd;
        std::mt19937 rng(rd());

        for(uint32_t i = 0;i < HASH_NUM;++i){
            keys[i] = new Key [LENGTH];
            memset(keys[i], 0, sizeof(Key) * LENGTH);

            counters[i] = new int32_t [LENGTH];
            memset(counters[i], 0, sizeof(int32_t) * LENGTH);
        }

        std::vector<Key> dataset;


        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        insert(dataset, keys, counters, *que, rng);
        end = now();
        std::cout << "Merge Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, keys, counters, *que, rng);
#endif

        (*finish) += 1;
    }


    inline static void insert(const std::vector<Key>& dataset, Key** keys, int32_t** counters, myQueue& q, std::mt19937& rng){
        uint32_t start, finish;
        uint32_t number = 1, length = dataset.size();
        uint32_t choice;
        uint16_t pos[2];

#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            *((uint32_t*)pos) = hash(dataset[i], 0);

            if(keys[0][pos[0]] == dataset[i]){
                counters[0][pos[0]] += 1;
                goto MergeEnd;
            }

            if(keys[1][pos[1]] == dataset[i]){
                counters[1][pos[1]] += 1;
                goto MergeEnd;
            }

            choice = (counters[0][pos[0]] > counters[1][pos[1]]);
            counters[choice][pos[choice]] += 1;
            if(rng() % counters[choice][pos[choice]] == 0){
                keys[choice][pos[choice]] = dataset[i];
            }

            MergeEnd:
            if((number % (INTERVAL * thread_num)) == 0){
                q.enqueue(Entry(keys, counters));

                keys = new Key* [HASH_NUM];
                counters = new int32_t* [HASH_NUM];

                for(uint32_t i = 0;i < HASH_NUM;++i){
                    keys[i] = new Key [LENGTH];
                    memset(keys[i], 0, sizeof(Key) * LENGTH);

                    counters[i] = new int32_t [LENGTH];
                    memset(counters[i], 0, sizeof(int32_t) * LENGTH);
                }
            }
            number += 1;
        }

        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] keys[i];
            delete [] counters[i];
        }

        delete [] keys;
        delete [] counters;
    }

    inline static void collect(Key** keys, int32_t** counters, myQueue* que, SIGNAL& finish){
        Entry temp;

        std::random_device rd;
        std::mt19937 rng(rd());

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                if(que[i].try_dequeue(temp)){
                    collect(keys, counters, temp, rng);
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                collect(keys, counters, temp, rng);
            }
        }
    }

    inline static void collect(Key** keys, int32_t** counters, const Entry& temp, std::mt19937& rng){
        for(uint32_t i = 0;i < HASH_NUM;++i){
            for(uint32_t j = 0;j < LENGTH;++j){
                counters[i][j] += temp.counters[i][j];
                if(counters[i][j] != 0 && rng() % counters[i][j] < temp.counters[i][j]){
                    keys[i][j] = temp.keys[i][j];
                }
            }
            delete [] temp.keys[i];
            delete [] temp.counters[i];
        }
        delete [] temp.keys;
        delete [] temp.counters;
    }
};

#endif
