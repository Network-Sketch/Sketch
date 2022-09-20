#ifndef DD_OURS_H
#define DD_OURS_H

#include "config.h"
#include "template/Ours.h"

template<typename Key, uint32_t thread_num>
class DD_Ours : public Ours<Key, DD_Entry<Key>, thread_num>{
public:

    typedef ReaderWriterQueue<DD_Entry<Key>> myQueue;

    Sketch<Key>* initialize_parent(){
        return new MyDD<Key>();
    }

    Sketch<Key>* initialize_child(){
       return new MyChild_DD<Key>();
    }

    void insert_child(Sketch<Key>* p, myQueue& q, const Key& packet){
        auto sketch = ((MyChild_DD<Key>*)p)->sketch;
        uint32_t pos = log(packet) / log(GAMMA);
        if (pos >= LENGTH) {
            pos = LENGTH - 1;
        }

        sketch[pos] += 1;
        if(sketch[pos] >= PROMASK){
            q.enqueue(DD_Entry<Key>(pos, sketch[pos]));
            sketch[pos] = 0;
        }
    }

    void merge(Sketch<Key>* p, DD_Entry<Key> temp){
        ((MyDD<Key>*)p)->Merge(temp);
    }

};

#endif