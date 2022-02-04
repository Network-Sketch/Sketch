#ifndef MERGE_H
#define MERGE_H

#include "Abstract.h"

template<uint32_t thread_num>
class Merge : public Abstract{
public:

    typedef Heap<uint32_t, int32_t> myHeap;

    struct Entry{
        HLL** sketch;
        myHeap* heap;

        Entry(HLL** _sketch = nullptr, myHeap* _heap = nullptr):
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

        double distinct[HASH_NUM] = {0};
        HLL* sketch[HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new HLL [LENGTH];
            memset(sketch[i], 0, sizeof(HLL) * LENGTH);
        }
        auto heap = new myHeap(HEAP_SIZE);

        std::thread thd[thread_num];
        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(ChildThread, &(thd[i]), i, start, size, &(que[i]), &finish);
        }

        collect(sketch, heap, que, finish, distinct);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Merge Accuracy:" << std::endl;
        HHCompare(heap->AllQuery(), (*mp));
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
        double distinct[HASH_NUM] = {0};
        HLL** sketch = new HLL* [HASH_NUM];
        for(uint32_t i = 0;i < HASH_NUM;++i){
            sketch[i] = new HLL [LENGTH];
            memset(sketch[i], 0, sizeof(HLL) * LENGTH);
        }
        auto heap = new myHeap(HEAP_SIZE);

        std::vector<Packet> dataset;

        Partition<thread_num>((Packet*)start, size / sizeof(Packet), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
        insert(dataset, sketch, heap, distinct, *que);
        end = now();
        std::cout << "Merge Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#else
        insert(dataset, sketch, heap, distinct, *que);
#endif

        (*finish) += 1;
    }


    inline static void insert(const std::vector<Packet>& dataset, HLL** sketch, myHeap* heap, double* distinct, myQueue& q){
        uint32_t start, finish;
        uint32_t number = 1, length = dataset.size();

#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            double estimation[HASH_NUM] = {0};
            for(uint32_t hashPos = 0;hashPos < HASH_NUM;++hashPos){
                uint32_t pos = hash(dataset[i].src, hashPos) % LENGTH;
                distinct[hashPos] -= sketch[hashPos][pos].Query();
                sketch[hashPos][pos].Insert(dataset[i].dst, hashPos);
                double est = sketch[hashPos][pos].Query();
                estimation[hashPos] = est - distinct[hashPos] / (LENGTH - 1);
                distinct[hashPos] += est;
            }
            heap->Insert(dataset[i].src, MEDIAN3(estimation));

            if((number % (INTERVAL * thread_num)) == 0){
                q.enqueue(Entry(sketch, heap));
                sketch = new HLL* [HASH_NUM];
                for(uint32_t i = 0;i < HASH_NUM;++i){
                    sketch[i] = new HLL [LENGTH];
                    memset(sketch[i], 0, sizeof(HLL) * LENGTH);
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

    inline static void collect(HLL** sketch, myHeap* heap, myQueue* que, SIGNAL& finish, double* distinct){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                if(que[i].try_dequeue(temp)){
                    collect(sketch, heap, temp, distinct);
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                collect(sketch, heap, temp, distinct);
            }
        }
    }

    inline static void collect(HLL** sketch, myHeap* heap, const Entry& temp, double* distinct){
        for(uint32_t i = 0;i < HASH_NUM;++i){
            distinct[i] = 0;
            for(uint32_t j = 0;j < LENGTH;++j){
                sketch[i][j].Merge(temp.sketch[i][j]);
                distinct[i] += sketch[i][j].Query();
            }
            delete [] temp.sketch[i];
        }
        delete [] temp.sketch;

        myHeap* check[2] = {heap, temp.heap};

        for(auto p : check) {
            for (uint32_t i = 0; i < p->mp->size(); ++i) {
                double estimation[HASH_NUM] = {0};
                for (uint32_t hashPos = 0; hashPos < HASH_NUM; ++hashPos) {
                    uint32_t pos = hash(p->heap[i].key, hashPos) % LENGTH;
                    double est = sketch[hashPos][pos].Query();
                    estimation[hashPos] = est - (distinct[hashPos] - est) / (LENGTH - 1);
                }
                heap->Insert(p->heap[i].key, MEDIAN3(estimation));
            }
        }

        delete temp.heap;
    }
};

#endif
