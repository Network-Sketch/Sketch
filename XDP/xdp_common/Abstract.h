#ifndef XDP_ABSTRACT_H
#define XDP_ABSTRACT_H

#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <net/if.h>
#include <linux/if_link.h>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <xdp/libxdp.h>

#include "../../common/Util.h"

#define UPPER_LENGTH 0xa0
#define TARGET_LENGTH 0x80
#define LOWER_LENGTH 0x60

struct Length{
    uint64_t nanoseconds;
    uint64_t length;
};

static const uint32_t zero = 0;
static int ncpus = 0;
/* This function will count the per-CPU number of packets and print out
 * the total number of dropped packets number and PPS (packets per second).
*/
void poll_stats(int fd){
    ncpus = libbpf_num_possible_cpus();
    uint64_t* values = new uint64_t [ncpus];
    uint64_t* prev = new uint64_t [ncpus];
    uint64_t sum = 0;

    memset(values, 0, sizeof(uint64_t) * ncpus);
    memset(prev, 0, sizeof(uint64_t) * ncpus);

    printf("CPU: %d\n", ncpus);

    TP last_time = now(), t;

    while (1) {
        t = now();

        double seconds = durationms(t, last_time);

        if(seconds >= 1000000){
            sum = 0;
            bpf_map_lookup_elem(fd, &zero, values);
            for (uint32_t i = 0; i < ncpus; i++){
                sum += (values[i] - prev[i]);
                if(values[i] - prev[i] > 0){
                    printf("%d : %10lu pkt/s\n", i, values[i] - prev[i]);
                }
            }

            printf("%10lu pkt/s\n", sum);
            memcpy(prev, values, sizeof(uint64_t) * ncpus);
            last_time = t;
        }
    }

    delete [] values;
    delete [] prev;
}

class Abstract{
public:

    virtual int32_t merge() = 0;

    int update(){
        if(xdp_load() < 0)
            return -1;

        std::thread stats;
        stats = std::thread(poll_stats, stats_fd);

        merge();

        stats.join();
        return 0;
    }

    int32_t xdp_load(){
        int32_t ifindex = if_nametoindex(nic);
        if (!ifindex) {
            printf("get ifindex from interface name failed\n");
            return -1;
        }

        /* load XDP object by libxdp */
        struct xdp_program * prog = xdp_program__open_file("prog.o", "prog", NULL);
        if (!prog) {
            printf("Error, load xdp prog failed\n");
            return -1;
        }

        /* attach XDP program to interface with skb mode
        * Please set ulimit if you got an -EPERM error.
        */
        if (xdp_program__attach(prog, ifindex, XDP_MODE_SKB, 0)) {
            printf("Error, Set xdp fd on %d failed\n", ifindex);
            return -1;
        }

        /* Find the map fd from the bpf object */
        bpf_obj = xdp_program__bpf_obj(prog);
        
        stats_fd = bpf_object__find_map_fd_by_name(bpf_obj, "packets");
        thd_fd = bpf_object__find_map_fd_by_name(bpf_obj, "threshold");
        buf_fd = bpf_object__find_map_fd_by_name(bpf_obj, "buffer");
        len_fd = bpf_object__find_map_fd_by_name(bpf_obj, "buffer_length");

        if (stats_fd < 0 || thd_fd < 0 || buf_fd < 0 || len_fd < 0) {
            printf("Error, get stats/thd fd from bpf obj failed\n");
            return -1;
        }

        int32_t threshold = 64;
        bpf_map_update_elem(thd_fd, &zero, &threshold, BPF_EXIST);

        return 0;
    }

    const char nic[13] = "enp65s0f0np0";

    struct bpf_object *bpf_obj;
    int32_t stats_fd, thd_fd, buf_fd, len_fd;
};

#endif