#ifndef OURS_H
#define OURS_H

#include "Abstract.h"

template<typename Key, typename Entry, uint32_t thread_num>
class Ours : public Abstract{
public:

    typedef ReaderWriterQueue<Entry> myQueue;

    myQueue que[thread_num];

    virtual Sketch<Key>* initialize_parent() = 0;
    virtual Sketch<Key>* initialize_child() = 0;

    virtual void merge(Sketch<Key>* sketch, Entry temp) = 0;

    virtual void insert_child(Sketch<Key>* sketch, myQueue& q, const Key& packet) = 0;

    void update(void* start, uint32_t size, HashMap mp){
        std::thread parent;
        parent = std::thread(&Ours::ParentThread, this, &parent, start, size, &mp);
        parent.join();
    }

    void ParentThread(std::thread* thisThd, void* start, uint32_t size, HashMap* mp){
#ifdef __linux__
        if(!setaffinity(thisThd, thread_num))
            return;
#endif
        std::atomic<int32_t> finish(0);
        std::thread thd[thread_num];

        Sketch<Key>* sketch = initialize_parent();

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i] = std::thread(&Ours::ChildThread, this,
                &(thd[i]), i, start, size, &finish);
        }

        collect(sketch, finish);

        for(uint32_t i = 0;i < thread_num;++i){
            thd[i].join();
        }

#ifdef ACCURACY
        std::cout << "Ours Accuracy:" << std::endl;
        HashMap ret = sketch->query_all();
        HHCompare(ret, (*mp), size / sizeof(Key) * ALPHA);
#endif
        delete sketch;
    }

    void ChildThread(std::thread* thisThd, uint32_t thread_id, void* start, uint32_t size,
            std::atomic<int32_t>* finish)
            {
#ifdef __linux__
        if(!setaffinity(thisThd, thread_id))
            return;
#endif
        Sketch<Key>* sketch = initialize_child();

        std::vector<Key> dataset;

        Partition<Key, thread_num>((Key*)start, size / sizeof(Key), thread_id, dataset);

#ifdef THROUGHPUT
        TP begin, end;
        begin = now();
#endif
        sketch = insert(dataset, sketch, que[thread_id]);
#ifdef THROUGHPUT
        end = now();
        std::cout << "Ours Insertion Throughput for " << thread_id << " : "
                  << dataset.size() * thread_num * THP_TIME / durationms(end, begin) << std::endl;
#endif

        (*finish)++;
        delete sketch;
    }

    Sketch<Key>* insert(const std::vector<Key>& dataset, Sketch<Key>* sketch, myQueue& q){
        uint32_t length = dataset.size();


#ifdef THROUGHPUT
        for(uint32_t t = 0;t < THP_TIME;++t)
#endif
        for(uint32_t i = 0;i < length;++i){
            insert_child(sketch, q, dataset[i]);
        }
        return sketch;
    }

    void collect(Sketch<Key>* sketch, std::atomic<int32_t>& finish){
        Entry temp;

        while(finish < thread_num){
            for(uint32_t i = 0;i < thread_num;++i){
                while(que[i].try_dequeue(temp)){
                    merge(sketch, temp);   
                }
            }
        }

        for(uint32_t i = 0;i < thread_num;++i){
            while(que[i].try_dequeue(temp)){
                merge(sketch, temp);
            }
        }
    }

};

#endif