#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define Coco_Merge_HASH_NUM 2
#define Coco_Merge_LENGTH (1 << 16)

struct Coco_Merge_Entry{
    Key** keys;
    int32_t** counters;

    Coco_Merge_Entry(Key** _keys = nullptr, int32_t** _counters = nullptr):
            keys(_keys), counters(_counters){};
};

typedef ReaderWriterQueue<Coco_Merge_Entry> Coco_Merge_Queue;
static Coco_Merge_Queue coco_merge_que[NUM_RX_QUEUE];

static void
coco_merge_coordinator()
{
	Coco_Merge_Entry temp;

    std::random_device rd;
    std::mt19937 rng(rd());

	Key* keys[Coco_Merge_HASH_NUM];
    int32_t* counters[Coco_Merge_HASH_NUM];

    for(uint32_t i = 0;i < Coco_Merge_HASH_NUM;++i){
        keys[i] = new Key [Coco_Merge_LENGTH];
        memset(keys[i], 0, sizeof(Key) * Coco_Merge_LENGTH);

        counters[i] = new int32_t [Coco_Merge_LENGTH];
        memset(counters[i], 0, sizeof(int32_t) * Coco_Merge_LENGTH);
    }

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            if(coco_merge_que[i].try_dequeue(temp)){
                
				for(uint32_t j = 0;j < Coco_Merge_HASH_NUM;++j){
                    for(uint32_t k = 0;k < Coco_Merge_LENGTH;++k){
                        counters[j][k] += temp.counters[j][k];
                        if(counters[j][k] != 0 && rng() % counters[j][k] < temp.counters[j][k]){
                            keys[j][k] = temp.keys[j][k];
                        }
                    }
                    delete [] temp.keys[j];
                    delete [] temp.counters[j];
                }
                delete [] temp.keys;
                delete [] temp.counters;
            }
        }
	}
}

/* main processing loop */
static void
coco_merge_local(unsigned queue_id)
{
	struct rte_mbuf *pkts_burst[MAX_PKT_BURST];
	struct rte_mbuf *m;
	int sent;
	uint64_t prev_tsc, diff_tsc, cur_tsc, timer_tsc;
	unsigned portid = 0;
	unsigned i, j, nb_rx;

	struct rte_eth_dev_tx_buffer *buffer;

	prev_tsc = 0;
	timer_tsc = 0;

    Key item[MAX_PKT_BURST];
	Key** keys;
    int32_t** counters;

    keys = new Key* [Coco_Merge_HASH_NUM];
    counters = new int32_t* [Coco_Merge_HASH_NUM];
    uint32_t choice;
    uint16_t pos[Coco_Merge_HASH_NUM];

    std::random_device rd;
    std::mt19937 rng(rd());

    for(uint32_t i = 0;i < Coco_Merge_HASH_NUM;++i){
        keys[i] = new Key [Coco_Merge_LENGTH];
        memset(keys[i], 0, sizeof(Key) * Coco_Merge_LENGTH);

        counters[i] = new int32_t [Coco_Merge_LENGTH];
        memset(counters[i], 0, sizeof(int32_t) * Coco_Merge_LENGTH);
    }

	uint64_t number = 0;
    uint64_t start, end;

	RTE_LOG(INFO, L2FWD, "entering main loop on queue %u\n", queue_id);

	while (!force_quit) {

#ifdef CYCLES
		start = rte_get_tsc_cycles();
#endif

		/* Read packet from RX queues. 8< */
		nb_rx = rte_eth_rx_burst(portid, queue_id,
						pkts_burst, MAX_PKT_BURST);

		port_statistics[queue_id].rx += nb_rx;

		for (j = 0; j < nb_rx; j++) {
			m = pkts_burst[j];
			rte_prefetch0(rte_pktmbuf_mtod(m, void *));
			rte_pktmbuf_adj(m, (uint16_t)sizeof(struct rte_ether_hdr));	
			struct rte_ipv4_hdr *ip_hdr = rte_pktmbuf_mtod(m, struct rte_ipv4_hdr *);
			
			Key src = rte_be_to_cpu_32(ip_hdr->src_addr);
			Key dst = rte_be_to_cpu_32(ip_hdr->dst_addr);
			item[j] = ((src << 32) | dst);

            rte_pktmbuf_free(m);
        }

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].poll_cycles += (end - start);
		start = rte_get_tsc_cycles();
#endif

        for (j = 0; j < nb_rx; j++) {
            *((uint32_t*)pos) = hash(item[j], 0);

            if(keys[0][pos[0]] == item[j]){
                counters[0][pos[0]] += 1;
                goto CocoMergeEnd;
            }

            if(keys[1][pos[1]] == item[j]){
                counters[1][pos[1]] += 1;
                goto CocoMergeEnd;
            }

            choice = (counters[0][pos[0]] > counters[1][pos[1]]);
            counters[choice][pos[choice]] += 1;
            if(rng() % counters[choice][pos[choice]] == 0){
                keys[choice][pos[choice]] = item[j];
            }

            CocoMergeEnd:
            if((number % (100000 * NUM_RX_QUEUE) == 0)){
                coco_merge_que[queue_id].enqueue(Coco_Merge_Entry(keys, counters));

                keys = new Key* [Coco_Merge_HASH_NUM];
                counters = new int32_t* [Coco_Merge_HASH_NUM];

                for(uint32_t i = 0;i < Coco_Merge_HASH_NUM;++i){
                    keys[i] = new Key [Coco_Merge_LENGTH];
                    memset(keys[i], 0, sizeof(Key) * Coco_Merge_LENGTH);

                    counters[i] = new int32_t [Coco_Merge_LENGTH];
                    memset(counters[i], 0, sizeof(int32_t) * Coco_Merge_LENGTH);
                }
            }

			number += 1;
		}

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].sketch_cycles += (end - start);
#endif
		/* >8 End of read packet from RX queues. */
	}
}

static int
coco_merge_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			coco_merge_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			coco_merge_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			coco_merge_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			coco_merge_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			coco_merge_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}