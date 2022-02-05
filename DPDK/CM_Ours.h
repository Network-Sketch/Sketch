#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define CM_Ours_HASH_NUM 3
#define CM_Ours_LENGTH (1 << 16)
#define CM_Ours_HEAP_SIZE 0x3ff

typedef Heap<Key, int32_t> CM_Ours_Heap;

struct CM_Ours_Entry{
    Key key;
    uint32_t pos;
    uint16_t value;

    CM_Ours_Entry(Key _key = 0, uint32_t _pos = 0, uint16_t _value = 0):
            key(_key), pos(_pos), value(_value){};
};

typedef ReaderWriterQueue<CM_Ours_Entry> CM_Ours_Queue;
static CM_Ours_Queue cm_ours_que[NUM_RX_QUEUE];

static void
cm_ours_coordinator()
{
	CM_Ours_Entry temp;

    int32_t* sketch[CM_Ours_HASH_NUM];
    CM_Ours_Heap* heap;

	for(uint32_t i = 0;i < CM_Ours_HASH_NUM;++i){
        sketch[i] = new int32_t [CM_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(int32_t) * CM_Ours_LENGTH);
    }

	heap = new CM_Ours_Heap(CM_Ours_HEAP_SIZE);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            while(cm_ours_que[i].try_dequeue(temp)){                
				uint32_t hashPos = temp.pos / CM_Ours_LENGTH, pos = temp.pos % CM_Ours_LENGTH;
                sketch[hashPos][pos] += temp.value;

                if(sketch[hashPos][pos] > heap->min()){
                    int32_t minimum = sketch[hashPos][pos];

                    for(uint32_t tempHash = 0;tempHash < CM_Ours_HASH_NUM;++tempHash){
                        uint32_t tempPos = hash(temp.key, tempHash) % CM_Ours_LENGTH;
                        minimum = MIN(minimum, sketch[tempHash][tempPos]);
                    }
                    heap->Insert(temp.key, minimum);
                }
            }
        }
	}
}

/* main processing loop */
static void
cm_ours_local(unsigned queue_id)
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
    uint16_t* sketch[CM_Ours_HASH_NUM];
	for(uint32_t i = 0;i < CM_Ours_HASH_NUM;++i){
        sketch[i] = new uint16_t [CM_Ours_LENGTH];
        memset(sketch[i], 0, sizeof(uint16_t) * CM_Ours_LENGTH);
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
            for(uint32_t hashPos = 0;hashPos < CM_Ours_HASH_NUM;++hashPos){
				uint32_t pos = hash(item[j], hashPos) % CM_Ours_LENGTH;
                sketch[hashPos][pos] += 1;
                if(sketch[hashPos][pos] >= 128){
                    cm_ours_que[queue_id].enqueue(CM_Ours_Entry(item[j], hashPos * CM_Ours_LENGTH + pos, 
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
cm_ours_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			cm_ours_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			cm_ours_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			cm_ours_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			cm_ours_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			cm_ours_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}
