#ifndef XDP_HEAP_H
#define XDP_HEAP_H

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/in.h>

#include <stdint.h>

#define HEAP_SIZE 0x3ff

#define MAX(a, b) (a > b? a:b)
#define MIN(a, b) (a < b? a:b)

struct KV{
    uint64_t key;
    int64_t value;
};

struct{
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __type(key, uint64_t);
    __type(value, uint32_t);
    __uint(max_entries, HEAP_SIZE);
} hashmap SEC(".maps");

struct{
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, struct KV);
    __uint(max_entries, HEAP_SIZE);
} heap SEC(".maps");

struct{
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, uint32_t);
    __uint(max_entries, 1);
} heap_size SEC(".maps");



int32_t heap_min(){
    uint32_t zero = 0;
    struct KV* kv = bpf_map_lookup_elem(&heap, &zero);
    if(!kv)
        return 0;
    else
        return kv->value;
}

int32_t heap_up(uint32_t pos) {
    for(uint32_t i = 0;i < 20;++i){
        if(pos <= 1)
            break;

        uint32_t parent = (pos - 1) / 2;

        struct KV* kv_parent = bpf_map_lookup_elem(&heap, &parent);
        struct KV* kv_pos = bpf_map_lookup_elem(&heap, &parent);

        if(!kv_parent || !kv_pos)
            return -1;

        if (kv_parent->value <= kv_pos->value)
            break;

        struct KV temp = *kv_pos;
        *kv_pos = *kv_parent;
        *kv_parent = temp;

        bpf_map_update_elem(&hashmap, &kv_pos->key, &pos, BPF_ANY);
        bpf_map_update_elem(&hashmap, &kv_parent->key, &parent, BPF_ANY);

        pos = parent;
    }

    return 0;
}

int32_t heap_down(uint32_t pos){
    uint32_t zero = 0;
    int32_t* size = bpf_map_lookup_elem(&heap_size, &zero);

    if(!size)
        return -1;

    uint32_t upper = *size;

    for(uint32_t i = 0;i < 20;++i){
        if(pos >= upper / 2)
            break;

        uint32_t left = 2 * pos + 1, right = 2 * pos + 2;
        uint32_t replace = pos;

        struct KV* kv_left = bpf_map_lookup_elem(&heap, &left);
        struct KV* kv_right = bpf_map_lookup_elem(&heap, &right);
        struct KV* kv_pos = bpf_map_lookup_elem(&heap, &replace);
        struct KV* kv_replace = kv_pos;

        if(!kv_left || !kv_right || !kv_pos)
            return -1;

        if (left < upper && kv_left->value < kv_pos->value){
            replace = left;
            kv_replace = kv_left;
        }
        if (right < upper && kv_right->value< kv_pos->value){
            replace = right;
            kv_replace = kv_right;
        }

        if (replace != pos) {
            struct KV temp = *kv_pos;
            *kv_pos = *kv_replace;
            *kv_replace = temp;
            bpf_map_update_elem(&hashmap, &kv_pos->key, &pos, BPF_ANY);
            bpf_map_update_elem(&hashmap, &kv_replace->key, &replace, BPF_ANY);
            pos = replace;
        }
        else return 0;
    }

    return 0;
}

int32_t heap_insert(uint64_t item, int32_t frequency){
    const uint32_t zero = 0;
    int32_t* size = bpf_map_lookup_elem(&heap_size, &zero);

    if(!size)
        return -1;

    struct KV insert_kv;
    if(*size >= HEAP_SIZE){
        struct KV* kv = bpf_map_lookup_elem(&heap, &zero);
        if(!kv)
            return -1;

        if(frequency > kv->value){
            uint32_t* index = bpf_map_lookup_elem(&hashmap, &item);
            if(index){
                insert_kv.key = item;
                insert_kv.value = frequency;
                bpf_map_update_elem(&heap, index, &insert_kv, BPF_ANY);
                heap_down(*index);
            }
            else{
                bpf_map_delete_elem(&hashmap, &kv->key);
                kv->value = frequency;
                kv->key = item;
                bpf_map_update_elem(&hashmap, &item, &zero, BPF_ANY);
                heap_down(0);
            }
        }
    }
    else{
        uint32_t* index = bpf_map_lookup_elem(&hashmap, &item);
        insert_kv.key = item;
        insert_kv.value = frequency;
        if(index){
            bpf_map_update_elem(&heap, index, &insert_kv, BPF_ANY);
            heap_down(*index);
        }
        else{
            bpf_map_update_elem(&heap, size, &insert_kv, BPF_ANY);
            bpf_map_update_elem(&hashmap, &item, size, BPF_ANY);
            heap_up(*size);
            *size += 1;
        }
    }

    return 0;
}

#endif
