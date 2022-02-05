#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "common/HLL.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define HLL_Ours_HASH_NUM 3
#define HLL_Ours_LENGTH (1 << 16)
#define HLL_Ours_HEAP_SIZE 0x3ff

typedef Heap<uint32_t, int32_t> HLL_Ours_Heap;

struct HLL_Ours_Entry{
    uint32_t src;
    uint32_t pos;
    uint32_t value;

    HLL_Ours_Entry(uint32_t _src = 0, uint32_t _pos = 0, uint32_t _value = 0):
            src(_src), pos(_pos), value(_value){};
};

typedef ReaderWriterQueue<HLL_Ours_Entry> HLL_Ours_Queue;
static HLL_Ours_Queue hll_ours_que[NUM_RX_QUEUE];

static void
hll_ours_coordinator()
{
	HLL_Ours_Entry temp;

	double distinct[HLL_Ours_HASH_NUM] = {0};
    HLL* sketch[HLL_Ours_HASH_NUM];
    for(uint32_t i = 0;i < HLL_Ours_HASH_NUM;++i){
        sketch[i] = new HLL [HLL_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(HLL) * HLL_Ours_LENGTH);
    }
    auto heap = new HLL_Ours_Heap(HLL_Ours_HEAP_SIZE);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            while(hll_ours_que[i].try_dequeue(temp)){
                
				uint32_t hashPos = temp.pos / HLL_Ours_LENGTH, pos = temp.pos % HLL_Ours_LENGTH;
                double before = sketch[hashPos][pos].Query();

                uint32_t inbucket_index = (temp.value & 0x1);
                uint32_t bucket_index = ((temp.value >> 1) & 0x7);
                uint8_t rank = MIN(Upper, __builtin_clz(temp.value) + 1);
                bool modify = false;

                Buckets& tempBucket = sketch[hashPos][pos].buckets[bucket_index];
                switch(inbucket_index){
                    case 0:
                        if(tempBucket.counter0 < rank){
                            tempBucket.counter0 = rank;
                            modify = true;
                            break;
                        }
                    case 1:
                        if(tempBucket.counter1 < rank){
                            tempBucket.counter1 = rank;
                            modify = true;
                            break;
                        }
                }

                if(modify){
                    double after = sketch[hashPos][pos].Query();
                    distinct[hashPos] += (after - before);
                    after = after - (distinct[hashPos] - after) / (HLL_Ours_LENGTH - 1);

                    if(after > heap->min()){
                        double estimation[HLL_Ours_HASH_NUM] = {0};

                        for(uint32_t tempHash = 0;tempHash < HLL_Ours_HASH_NUM;++tempHash){
                            uint32_t tempPos = hash(temp.src, tempHash) % HLL_Ours_LENGTH;
                            double est = sketch[tempHash][tempPos].Query();
                            estimation[tempHash] = est - (distinct[tempHash] - est) / (HLL_Ours_LENGTH - 1);
                        }
                        heap->Insert(temp.src, MEDIAN3(estimation));
                    }
                }
            }
        }
	}
}

/* main processing loop */
static void
hll_ours_local(unsigned queue_id)
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

	double distinct[HLL_Ours_HASH_NUM] = {0};
    HLL* sketch[HLL_Ours_HASH_NUM];
    for(uint32_t i = 0;i < HLL_Ours_HASH_NUM;++i){
        sketch[i] = new HLL [HLL_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(HLL) * HLL_Ours_LENGTH);
    }

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
			for(uint32_t hashPos = 0;hashPos < HLL_Ours_HASH_NUM;++hashPos){
                uint32_t pos = hash(src[j], hashPos) % HLL_Ours_LENGTH;
                uint32_t temp = hash(dst[j], hashPos);
                uint32_t inbucket_index = (temp & 0x1);
                uint32_t bucket_index = ((temp >> 1) & 0x7);
                uint8_t rank = MIN(Upper, __builtin_clz(temp) + 1);

                Buckets& tempBucket = sketch[hashPos][pos].buckets[bucket_index];

                switch(inbucket_index){
                    case 0:
                        if(tempBucket.counter0 < rank){
                            tempBucket.counter0 = rank;
                            hll_ours_que[queue_id].enqueue(HLL_Ours_Entry(src[j], hashPos * HLL_Ours_LENGTH + pos, temp));
                        }
                        break;
                    case 1:
                        if(tempBucket.counter1 < rank){
                            tempBucket.counter1 = rank;
                            hll_ours_que[queue_id].enqueue(HLL_Ours_Entry(src[j], hashPos * HLL_Ours_LENGTH + pos, temp));
                        }
                        break;
                }
            }

		}

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].sketch_cycles += (end - start);
#endif
		/* >8 End of read packet from RX queues. */
	}
}

static int
hll_ours_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			hll_ours_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			hll_ours_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			hll_ours_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			hll_ours_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			hll_ours_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}