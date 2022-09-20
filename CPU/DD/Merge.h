#ifndef DD_MERGE_H
#define DD_MERGE_H

#include "config.h"
#include "template/Merge.h"

template<typename Key, uint32_t thread_num>
class DD_Merge : public Merge<Key, MyDD<Key>*, thread_num>{
public:

    typedef ReaderWriterQueue<MyDD<Key>*> myQueue;

    Sketch<Key>* initialize_parent(){
        return new MyDD<Key>();
    }

    Sketch<Key>* initialize_child(){
        return initialize_parent();
    }

    Sketch<Key>* insert_child(Sketch<Key>* p, myQueue& q, const Key& packet, uint32_t number, uint32_t thread_id){
        p->insert_one(packet);

        if((number % (INTERVAL * thread_num)) == (INTERVAL * thread_id)){
            q.enqueue((MyDD<Key>*)p);
            return initialize_child();
        }
        return p;
    }

    void merge(Sketch<Key>* p, MyDD<Key>* temp){
        ((MyDD<Key>*)p)->Merge(temp);
    }

};

#endif