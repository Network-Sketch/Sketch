#include "../xdp_common/hash.h"
#include "../xdp_common/parse.h"

#include "../xdp_common/SHLL.h"

#include "XDP_Locher.h"

#define HASH_NUM 3
#define LENGTH 65536

struct Length{
    uint64_t nanoseconds;
    uint64_t length;
};

struct{
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, struct SHLL);
    __uint(max_entries, HASH_NUM * LENGTH);
} sketch SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, uint64_t);
    __uint(max_entries, 1);
} packets SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, int32_t);
    __uint(max_entries, 1);
} threshold SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024);
} buffer SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, struct Length);
    __uint(max_entries, 1);
} buffer_length SEC(".maps");

int32_t fill_entry(uint32_t _src, uint16_t _hashPos, uint16_t _pos,
                    uint32_t _value){
    struct Entry *entry = bpf_ringbuf_reserve(&buffer, sizeof(struct Entry), 0);
    if(entry){
        entry->src = _src;
        entry->hashPos = _hashPos;
        entry->pos = _pos;
        entry->value = _value;
        bpf_ringbuf_submit(entry, 0);
    }
    return 0;
}

int32_t my_clz(uint32_t number){
    int32_t ret = 0;
    for(uint32_t i = 0;i < 16;++i){
        if(number & 1){
            return ret;
        }
        else{
            ret += 1;
        }
    }
    return ret;
}

SEC("prog")
int sketch_prog(struct xdp_md *skb)
{
    struct Packet packet;
    
    if(parse_key(skb, &packet) < 0)
        return XDP_DROP;

    uint64_t *number = bpf_map_lookup_elem(&packets, &zero);
    if(number){
        *number += 1;
        if(((*number) & 0xff) == 0xff){
            struct Length *len = bpf_map_lookup_elem(&buffer_length, &zero);
            if(len){
                uint64_t ns = bpf_ktime_get_ns();
                if(ns > len->nanoseconds + 100000){
                    len->nanoseconds = ns;
                    len->length = bpf_ringbuf_query(&buffer, BPF_RB_AVAIL_DATA);
                }
            }
        }
    }

    uint32_t pos[HASH_NUM];
    uint32_t temp[HASH_NUM];

    for(uint32_t i = 0;i < HASH_NUM;++i){
        pos[i] = hash(packet.src, seed[i]) % (uint32_t)LENGTH + i * LENGTH;
        temp[i] = hash(packet.dst, seed[i]);
    }

    for(uint32_t i = 0;i < HASH_NUM;++i){
        uint32_t bucket_index = ((temp[i] >> 1) & 0x7);
        struct SHLL* shll = bpf_map_lookup_elem(&sketch, &pos[i]);

        if(shll){
            uint32_t inbucket_index = (temp[i] & 0x1);
            uint8_t rank = MIN(15, my_clz(temp[i]) + 1);

            switch(inbucket_index){
                case 0:
                    if(shll->buckets[bucket_index].counter0 < rank){
                        shll->buckets[bucket_index].counter0 = rank;
                        if(rank > 1)
                            fill_entry(packet.src, i, pos[i], temp[i]);
                    }
                    break;
                case 1:
                    if(shll->buckets[bucket_index].counter1 < rank){
                        shll->buckets[bucket_index].counter1 = rank;
                        if(rank > 1)
                            fill_entry(packet.src, i, pos[i], temp[i]);
                    }
                    break;
            }
        }
    }
        
    return XDP_DROP;
}

char _license[] SEC("license") = "GPL";
