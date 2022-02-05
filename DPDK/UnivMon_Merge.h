#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define UnivMon_Merge_MAX_LEVEL 6
#define UnivMon_Merge_HASH_NUM 3
#define UnivMon_Merge_LENGTH (1 << 16)
#define UnivMon_Merge_HEAP_SIZE 0x3ff

typedef Heap<Key, int32_t> UnivMon_Merge_Heap;

struct UnivMon_Merge_Entry{
    UnivMon_Merge_Heap** heap;
    int32_t*** sketch;

    UnivMon_Merge_Entry(UnivMon_Merge_Heap** _heap = nullptr, int32_t*** _sketch = nullptr):
            heap(_heap), sketch(_sketch){};
};

typedef ReaderWriterQueue<UnivMon_Merge_Entry> UnivMon_Merge_Queue;
static UnivMon_Merge_Queue univmon_merge_que[NUM_RX_QUEUE];

static void
univmon_merge_coordinator()
{
	UnivMon_Merge_Entry temp;

	int32_t* sketch[UnivMon_Merge_MAX_LEVEL][UnivMon_Merge_HASH_NUM];
    UnivMon_Merge_Heap* heap[UnivMon_Merge_MAX_LEVEL];

    for(uint32_t i = 0;i < UnivMon_Merge_MAX_LEVEL;++i){
        for(uint32_t j = 0;j < UnivMon_Merge_HASH_NUM;++j){
            sketch[i][j] = new int32_t [UnivMon_Merge_LENGTH];
            memset(sketch[i][j], 0, sizeof(int32_t) * UnivMon_Merge_LENGTH);
        }
        heap[i] = new UnivMon_Merge_Heap(UnivMon_Merge_HEAP_SIZE);
    }

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            if(univmon_merge_que[i].try_dequeue(temp)){
                
				for(uint32_t level = 0;level < UnivMon_Merge_MAX_LEVEL;++level){
                    for(uint32_t j = 0;j < UnivMon_Merge_HASH_NUM;++j){
                        for(uint32_t k = 0;k < UnivMon_Merge_LENGTH;++k){
                            sketch[level][j][k] += temp.sketch[level][j][k];
                        }
                        delete [] temp.sketch[level][j];
                    }
                    delete [] temp.sketch[level];

                    UnivMon_Merge_Heap* check[2] = {heap[level], temp.heap[level]};

                    for(auto p : check) {
                        for(uint32_t j = 0;j < p->mp->size();++j){
                            int32_t number[UnivMon_Merge_HASH_NUM] = {0};
                            for(uint32_t hashPos = 0;hashPos < UnivMon_Merge_HASH_NUM;++hashPos){
                                uint32_t hashNum = hash(p->heap[j].key, level * UnivMon_Merge_HASH_NUM + hashPos);
                                uint32_t pos = (hashNum >> 1) % UnivMon_Merge_LENGTH;
                                int32_t incre = increment[hashNum & 1];
                                number[hashPos] = sketch[level][hashPos][pos] * incre;
                            }
                            heap[level]->Insert(p->heap[j].key, MEDIAN3(number));
                        }
                    }

                    delete temp.heap[level];
                }

                delete [] temp.heap;
                delete [] temp.sketch;

            }
        }
	}
}

/* main processing loop */
static void
univmon_merge_local(unsigned queue_id)
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
	int32_t*** sketch;
    UnivMon_Merge_Heap** heap;

    sketch = new int32_t** [UnivMon_Merge_MAX_LEVEL];
    heap = new UnivMon_Merge_Heap* [UnivMon_Merge_MAX_LEVEL];

    for(uint32_t i = 0;i < UnivMon_Merge_MAX_LEVEL;++i){
        sketch[i] = new int32_t* [UnivMon_Merge_HASH_NUM];
        heap[i] = new UnivMon_Merge_Heap(UnivMon_Merge_HEAP_SIZE);

        for(uint32_t j = 0;j < UnivMon_Merge_HASH_NUM;++j){
            sketch[i][j] = new int32_t [UnivMon_Merge_LENGTH];
            memset(sketch[i][j], 0, sizeof(int32_t) * UnivMon_Merge_LENGTH);
        }
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
            uint32_t polar = hash(item[j], 199);
            uint32_t max_level = MIN(UnivMon_Merge_MAX_LEVEL - 1, __builtin_clz(polar));

            for(uint32_t level = 0; level <= max_level;++level){
                int32_t number[UnivMon_Merge_HASH_NUM] = {0};
                for(uint32_t hashPos = 0;hashPos < UnivMon_Merge_HASH_NUM;++hashPos){
                    uint32_t hashNum = hash(item[j], level * UnivMon_Merge_HASH_NUM + hashPos);
                    uint32_t pos = (hashNum >> 1) % UnivMon_Merge_LENGTH;
                    int32_t incre = increment[hashNum & 1];

                    sketch[level][hashPos][pos] += incre;
                    number[hashPos] = sketch[level][hashPos][pos] * incre;
                }
                heap[level]->Insert(item[j], MEDIAN3(number));
            }

            if((number % (100000 * NUM_RX_QUEUE)) == 0){
                univmon_merge_que[queue_id].enqueue(UnivMon_Merge_Entry(heap, sketch));

                sketch = new int32_t** [UnivMon_Merge_MAX_LEVEL];
                heap = new UnivMon_Merge_Heap* [UnivMon_Merge_MAX_LEVEL];

                for(uint32_t i = 0;i < UnivMon_Merge_MAX_LEVEL;++i){
                    sketch[i] = new int32_t* [UnivMon_Merge_HASH_NUM];
                    heap[i] = new UnivMon_Merge_Heap(UnivMon_Merge_HEAP_SIZE);

                    for(uint32_t k = 0;k < UnivMon_Merge_HASH_NUM;++k){
                        sketch[i][k] = new int32_t [UnivMon_Merge_LENGTH];
                        memset(sketch[i][k], 0, sizeof(int32_t) * UnivMon_Merge_LENGTH);
                    }
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
univmon_merge_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			univmon_merge_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			univmon_merge_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			univmon_merge_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			univmon_merge_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			univmon_merge_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}