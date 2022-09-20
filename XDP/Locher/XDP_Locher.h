#ifndef XDP_LOCHER_H
#define XDP_LOCHER_H

#include <stdint.h>

struct Entry{
    uint32_t src;
    uint16_t hashPos;
    uint16_t pos;
    uint32_t value;
};

#endif