#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "common/HLL.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define HLL_Merge_HASH_NUM 3
#define HLL_Merge_LENGTH (1 << 16)
#define HLL_Merge_HEAP_SIZE 0x3ff

typedef Heap<uint32_t, int32_t> HLL_Merge_Heap;

struct HLL_Merge_Entry{
    HLL** sketch;
    HLL_Merge_Heap* heap;

    HLL_Merge_Entry(HLL** _sketch = nullptr, HLL_Merge_Heap* _heap = nullptr):
                sketch(_sketch), heap(_heap){};
};

typedef ReaderWriterQueue<HLL_Merge_Entry> HLL_Merge_Queue;
static HLL_Merge_Queue hll_merge_que[NUM_RX_QUEUE];

static void
hll_merge_coordinator()
{
	HLL_Merge_Entry temp;

	double distinct[HLL_Merge_HASH_NUM] = {0};
    HLL* sketch[HLL_Merge_HASH_NUM];
    for(uint32_t i = 0;i < HLL_Merge_HASH_NUM;++i){
        sketch[i] = new HLL [HLL_Merge_LENGTH];
        memset(sketch[i], 0, sizeof(HLL) * HLL_Merge_LENGTH);
    }
    auto heap = new HLL_Merge_Heap(HLL_Merge_HEAP_SIZE);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            if(hll_merge_que[i].try_dequeue(temp)){
                
				for(uint32_t j = 0;j < HLL_Merge_HASH_NUM;++j){
                    distinct[j] = 0;
                    for(uint32_t k = 0;k < HLL_Merge_LENGTH;++k){
                        sketch[j][k].Merge(temp.sketch[j][k]);
                        distinct[j] += sketch[j][k].Query();
                    }
                    delete [] temp.sketch[j];
                }
                delete [] temp.sketch;

                HLL_Merge_Heap* check[2] = {heap, temp.heap};

                for(auto p : check) {
                    for (uint32_t j = 0; j < p->mp->size(); ++j) {
                        double estimation[HLL_Merge_HASH_NUM] = {0};
                        for (uint32_t hashPos = 0; hashPos < HLL_Merge_HASH_NUM; ++hashPos) {
                            uint32_t pos = hash(p->heap[j].key, hashPos) % HLL_Merge_LENGTH;
                            double est = sketch[hashPos][pos].Query();
                            estimation[hashPos] = est - (distinct[hashPos] - est) / (HLL_Merge_LENGTH - 1);
                        }
                        heap->Insert(p->heap[j].key, MEDIAN3(estimation));
                    }
                }

                delete temp.heap;
            }
        }
	}
}

/* main processing loop */
static void
hll_merge_local(unsigned queue_id)
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

	uint32_t src[MAX_PKT_BURST];
	uint32_t dst[MAX_PKT_BURST];

	double distinct[HLL_Merge_HASH_NUM] = {0};
	double estimation[HLL_Merge_HASH_NUM] = {0};
    HLL** sketch = new HLL* [HLL_Merge_HASH_NUM];
    for(uint32_t i = 0;i < HLL_Merge_HASH_NUM;++i){
        sketch[i] = new HLL [HLL_Merge_LENGTH];
        memset(sketch[i], 0, sizeof(HLL) * HLL_Merge_LENGTH);
    }
    auto heap = new HLL_Merge_Heap(HLL_Merge_HEAP_SIZE);

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
			
			src[j] = rte_be_to_cpu_32(ip_hdr->src_addr);
			dst[j] = rte_be_to_cpu_32(ip_hdr->dst_addr);

			rte_pktmbuf_free(m);
		}

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].poll_cycles += (end - start);
		start = rte_get_tsc_cycles();
#endif

		for (j = 0; j < nb_rx; j++) {
            for(uint32_t hashPos = 0;hashPos < HLL_Merge_HASH_NUM;++hashPos){
                uint32_t pos = hash(src[j], hashPos) % HLL_Merge_LENGTH;
                distinct[hashPos] -= sketch[hashPos][pos].Query();
                sketch[hashPos][pos].Insert(dst[j], hashPos);
                double est = sketch[hashPos][pos].Query();
                estimation[hashPos] = est - distinct[hashPos] / (HLL_Merge_LENGTH - 1);
                distinct[hashPos] += est;
            }
            heap->Insert(src[j], MEDIAN3(estimation));

            if((number % (100000 * NUM_RX_QUEUE)) == 0){
                hll_merge_que[queue_id].enqueue(HLL_Merge_Entry(sketch, heap));
                sketch = new HLL* [HLL_Merge_HASH_NUM];
                for(uint32_t i = 0;i < HLL_Merge_HASH_NUM;++i){
                    sketch[i] = new HLL [HLL_Merge_LENGTH];
                    memset(sketch[i], 0, sizeof(HLL) * HLL_Merge_LENGTH);
                }
                heap = new HLL_Merge_Heap(HLL_Merge_HEAP_SIZE);
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
hll_merge_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			hll_merge_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			hll_merge_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			hll_merge_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			hll_merge_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			hll_merge_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}