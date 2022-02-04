#ifndef MERGE_H
#define MERGE_H

#include "Abstract.h"

template<typename Key, uint32_t thread_num>
class Merge : public Abstract{
public:

    typedef Heap<Key, int32_t> myHeap;

    struct Entry{
        int32_t** sketch;
        myHeap* heap;

        Entry(int32_t** _sketch = nullptr, myHeap* _heap = nullptr):
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
        std::cout << "Merge Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), (*mp), size / sizeof(Key) * ALPHA);
#endif
        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete heap;
    }

    static void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
            myQueue* que, SIGNAL* finish)
            {
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        int32_t** sketch;
        sketch = new int32_t* [HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new int32_t [LENGTH];
            memset(sketch[i], 0, sizeof(int32_t) * LENGTH);
        }
        auto heap = new myHeap(HEAP_SIZE);

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


    inline static void insert(const std::vector<Key>& dataset, int32_t** sketch, myHeap* heap, myQueue& q){
        uint32_t start, finish;
        uint32_t number = 1, length = dataset.size();

#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            int32_t minimum = 0x7fffffff;
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i], hashPos) % LENGTH;
                sketch[hashPos][pos] += 1;
                minimum = MIN(minimum, sketch[hashPos][pos]);
            }
            heap->Insert(dataset[i], minimum);

            if((number % (INTERVAL * thread_num)) == 0){
                q.enqueue(Entry(sketch, heap));
                sketch = new int32_t* [HASH_NUM];
                for(uint32_t i = 0;i < HASH_NUM;++i){
                    sketch[i] = new int32_t [LENGTH];
                    memset(sketch[i], 0, sizeof(int32_t) * LENGTH);
                }
                heap = new myHeap(HEAP_SIZE);
            }
            number += 1;
        }

        for(uint32_t i = 0;i < HASH_NUM;++i){
            delete [] sketch[i];
        }
        delete [] sketch;
        delete heap;
    }

    inline static void collect(int32_t** sketch, myHeap* heap, myQueue* que, SIGNAL& finish){
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

    inline static void collect(int32_t** sketch, myHeap* heap, const Entry& temp){
        for(uint32_t i = 0;i < HASH_NUM;++i){
            for(uint32_t j = 0;j < LENGTH;++j){
                sketch[i][j] += temp.sketch[i][j];
            }
            delete [] temp.sketch[i];
        }
        delete [] temp.sketch;

        myHeap* check[2] = {heap, temp.heap};

        for(auto p : check){
            for(uint32_t i = 0;i < p->mp->size();++i){
                int32_t minimum = 0x7fffffff;
                for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                    uint32_t pos = hash(p->heap[i].key, hashPos) % LENGTH;
                    minimum = MIN(minimum, sketch[hashPos][pos]);
                }
                heap->Insert(p->heap[i].key, minimum);
            }
        }

        delete temp.heap;
    }
};

#endif
