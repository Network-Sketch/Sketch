#ifndef MERGE_H
#define MERGE_H

#include "Abstract.h"

template<typename Key, uint32_t thread_num>
class Merge : public Abstract{
public:

    typedef Heap<Key, int32_t> myHeap;

    struct Entry{
        int32_t*** sketch;
        myHeap** heap;

        Entry(int32_t*** _sketch = nullptr, myHeap** _heap = nullptr):
                sketch(_sketch), heap(_heap){};
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
        std::cout << "Merge Accuracy:" << std::endl;

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
            myQueue* que, SIGNAL* finish)
            {
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        int32_t*** sketch;
        myHeap** heap;

        sketch = new int32_t** [MAX_LEVEL];
        heap = new myHeap* [MAX_LEVEL];

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            sketch[i] = new int32_t* [HASH_NUM];
            heap[i] = new myHeap(HEAP_SIZE);

            for(uint32_t j = 0;j < HASH_NUM;++j){
                sketch[i][j] = new int32_t [LENGTH];
                memset(sketch[i][j], 0, sizeof(int32_t) * LENGTH);
            }
        }

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        insert(dataset, sketch, heap, *que);
        end = now();
        std::cout << "Merge Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, heap, *que);
#endif

        (*finish) += 1;
    }


    inline static void insert(const std::vector<Key>& dataset, int32_t*** sketch, myHeap** heap, myQueue& q){
        uint32_t start, finish;
        uint32_t number = 1, length = dataset.size();

#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            uint32_t polar = hash(dataset[i], 199);
            uint32_t max_level = MIN(MAX_LEVEL - 1, __builtin_clz(polar));

            for(uint32_t level = 0; level <= max_level;++level){
                int32_t number[HASH_NUM] = {0};
                for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                    uint32_t hashNum = hash(dataset[i], level * HASH_NUM + hashPos);
                    uint32_t pos = (hashNum >> 1) % LENGTH;
                    int32_t incre = increment[hashNum & 1];

                    sketch[level][hashPos][pos] += incre;
                    number[hashPos] = sketch[level][hashPos][pos] * incre;
                }
                heap[level]->Insert(dataset[i], MEDIAN3(number));
            }

            if((number % (INTERVAL * thread_num)) == 0){
                q.enqueue(Entry(sketch, heap));

                sketch = new int32_t** [MAX_LEVEL];
                heap = new myHeap* [MAX_LEVEL];

                for(uint32_t i = 0;i < MAX_LEVEL;++i){
                    sketch[i] = new int32_t* [HASH_NUM];
                    heap[i] = new myHeap(HEAP_SIZE);

                    for(uint32_t j = 0;j < HASH_NUM;++j){
                        sketch[i][j] = new int32_t [LENGTH];
                        memset(sketch[i][j], 0, sizeof(int32_t) * LENGTH);
                    }
                }
            }

            number += 1;
        }

        for(uint32_t i = 0;i < MAX_LEVEL;++i){
            for(uint32_t j = 0;j < HASH_NUM;++j){
                delete [] sketch[i][j];
            }
            delete [] sketch[i];
            delete heap[i];
        }

        delete [] sketch;
        delete [] heap;
    }

    inline static void collect(int32_t* sketch[MAX_LEVEL][HASH_NUM], myHeap* heap[MAX_LEVEL], myQueue* que, SIGNAL& finish){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                if(que[i].try_dequeue(temp)){
                    collect(sketch, heap, temp);
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                collect(sketch, heap, temp);
            }
        }
    }

    inline static void collect(int32_t* sketch[MAX_LEVEL][HASH_NUM], myHeap* heap[MAX_LEVEL], const Entry& temp){
        for(uint32_t level = 0;level < MAX_LEVEL;++level){
            for(uint32_t i = 0;i < HASH_NUM;++i){
                for(uint32_t j = 0;j < LENGTH;++j){
                    sketch[level][i][j] += temp.sketch[level][i][j];
                }
                delete [] temp.sketch[level][i];
            }
            delete [] temp.sketch[level];

            myHeap* check[2] = {heap[level], temp.heap[level]};

            for(auto p : check) {
                for(uint32_t i = 0;i < p->mp->size();++i){
                    int32_t number[HASH_NUM] = {0};
                    for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                        uint32_t hashNum = hash(p->heap[i].key, level * HASH_NUM + hashPos);
                        uint32_t pos = (hashNum >> 1) % LENGTH;
                        int32_t incre = increment[hashNum & 1];
                        number[hashPos] = sketch[level][hashPos][pos] * incre;
                    }
                    heap[level]->Insert(p->heap[i].key, MEDIAN3(number));
                }
            }

            delete temp.heap[level];
        }

        delete [] temp.heap;
        delete [] temp.sketch;
    }
};

#endif
