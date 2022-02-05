#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define Count_Ours_HASH_NUM 3
#define Count_Ours_LENGTH (1 << 16)
#define Count_Ours_HEAP_SIZE 0x3ff

typedef Heap<Key, int32_t> Count_Ours_Heap;

struct Count_Ours_Entry{
    Key key;
    uint32_t pos;
    int16_t value;

    Count_Ours_Entry(Key _key = 0, uint32_t _pos = 0, int16_t _value = 0):
            key(_key), pos(_pos), value(_value){};
};

typedef ReaderWriterQueue<Count_Ours_Entry> Count_Ours_Queue;
static Count_Ours_Queue count_ours_que[NUM_RX_QUEUE];

static void
count_ours_coordinator()
{
	Count_Ours_Entry temp;

    int32_t* sketch[Count_Ours_HASH_NUM];
    Count_Ours_Heap* heap;

	for(uint32_t i = 0;i < Count_Ours_HASH_NUM;++i){
        sketch[i] = new int32_t [Count_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(int32_t) * Count_Ours_LENGTH);
    }

	heap = new Count_Ours_Heap(Count_Ours_HEAP_SIZE);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            while(count_ours_que[i].try_dequeue(temp)){                
				uint32_t hashPos = temp.pos / Count_Ours_LENGTH, pos = temp.pos % Count_Ours_LENGTH;
                sketch[hashPos][pos] += temp.value;

                if(abs(sketch[hashPos][pos]) > heap->min()){
                    int32_t count[Count_Ours_HASH_NUM] = {0};

                    for(uint32_t tempHash = 0;tempHash < Count_Ours_HASH_NUM;++tempHash){
                        uint32_t hashNum = hash(temp.key, tempHash);
                        uint32_t tempPos = (hashNum >> 1) % Count_Ours_LENGTH;
                        int32_t incre = increment[hashNum & 1];

                        count[tempHash] = sketch[tempHash][tempPos] * incre;
                    }
                    heap->Insert(temp.key, MEDIAN3(count));
                }
            }
        }
	}
}

/* main processing loop */
static void
count_ours_local(unsigned queue_id)
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
    int16_t* sketch[Count_Ours_HASH_NUM];
	for(uint32_t i = 0;i < Count_Ours_HASH_NUM;++i){
        sketch[i] = new int16_t [Count_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(int16_t) * Count_Ours_LENGTH);
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
            for(uint32_t hashPos = 0;hashPos < Count_Ours_HASH_NUM;++hashPos){
				uint32_t hashNum = hash(item[j], hashPos);
                uint32_t pos = (hashNum >> 1) % Count_Ours_LENGTH;
                int32_t incre = increment[hashNum & 1];

                sketch[hashPos][pos] += incre;
                if(sketch[hashPos][pos] * incre >= 128){
                    count_ours_que[queue_id].enqueue(Count_Ours_Entry(item[j], hashPos * Count_Ours_LENGTH + pos, 
                        sketch[hashPos][pos]));
                    sketch[hashPos][pos] = 0;
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
count_ours_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			count_ours_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			count_ours_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			count_ours_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			count_ours_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			count_ours_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}