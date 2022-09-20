#ifndef DD_IDEAL_H
#define DD_IDEAL_H

#include "config.h"
#include "template/Ideal.h"

template<typename Key>
class DD_Ideal : public Ideal<Key>{
public:
    Sketch<Key>* initialize_sketch(){
        return new MyDD<Key>();
    }
};

#endif