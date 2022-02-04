#ifndef UTIL_H
#define UTIL_H

#include <chrono>

#define MAX(a, b) (a > b? a:b)
#define MIN(a, b) (a < b? a:b)

typedef std::chrono::high_resolution_clock::time_point TP;

struct Packet{
    uint32_t src;
    uint32_t dst;

    operator uint64_t() const{
        return *((uint64_t*)(this));
    }
};

inline TP now(){
    return std::chrono::high_resolution_clock::now();
}

inline double durationms(TP finish, TP start){
    return std::chrono::duration_cast<std::chrono::duration<double,std::ratio<1,1000000>>>(finish - start).count();
}

inline double durationns(TP finish, TP start){
    return std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
}

#endif
