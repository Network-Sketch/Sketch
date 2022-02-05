#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define Coco_Ours_HASH_NUM 2
#define Coco_Ours_LENGTH (1 << 16)

struct Coco_Ours_Entry{
    Key key;
    uint16_t pos[2];
    uint16_t value;

    Coco_Ours_Entry(Key _key = 0, uint16_t _pos0 = 0, uint16_t _pos1 = 0, uint16_t _value = 0):
            key(_key), value(_value){
        pos[0] = _pos0;
        pos[1] = _pos1;
    };
};

typedef ReaderWriterQueue<Coco_Ours_Entry> Coco_Ours_Queue;
static Coco_Ours_Queue coco_ours_que[NUM_RX_QUEUE];

static void
coco_ours_coordinator()
{
	Coco_Ours_Entry temp;

    std::random_device rd;
    std::mt19937 rng(rd());

	Key* keys[Coco_Ours_HASH_NUM];
    int32_t* counters[Coco_Ours_HASH_NUM];

    for(uint32_t i = 0;i < Coco_Ours_HASH_NUM;++i){
        keys[i] = new Key [Coco_Ours_LENGTH];
        memset(keys[i], 0, sizeof(Key) * Coco_Ours_LENGTH);

        counters[i] = new int32_t [Coco_Ours_LENGTH];
        memset(counters[i], 0, sizeof(int32_t) * Coco_Ours_LENGTH);
    }

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            while(coco_ours_que[i].try_dequeue(temp)){
                
				if(keys[0][temp.pos[0]] == temp.key){
                    counters[0][temp.pos[0]] += temp.value;
                    continue;
                }

                if(keys[1][temp.pos[1]] == temp.key){
                    counters[1][temp.pos[1]] += temp.value;
                    continue;
                }

                uint32_t choice = (counters[0][temp.pos[0]] > counters[1][temp.pos[1]]);
                counters[choice][temp.pos[choice]] += temp.value;
                if(rng() % counters[choice][temp.pos[choice]] < temp.value){
                    keys[choice][temp.pos[choice]] = temp.key;
                }
            }
        }
	}
}

/* main processing loop */
static void
coco_ours_local(unsigned queue_id)
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
	Key* keys[Coco_Ours_HASH_NUM];
    uint16_t* counters[Coco_Ours_HASH_NUM];

    uint32_t choice;
    uint16_t pos[Coco_Ours_HASH_NUM];

    std::random_device rd;
    std::mt19937 rng(rd());

    for(uint32_t i = 0;i < Coco_Ours_HASH_NUM;++i){
        keys[i] = new Key [Coco_Ours_LENGTH];
        memset(keys[i], 0, sizeof(Key) * Coco_Ours_LENGTH);

        counters[i] = new uint16_t [Coco_Ours_LENGTH];
        memset(counters[i], 0, sizeof(uint16_t) * Coco_Ours_LENGTH);
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
            *((uint32_t*)pos) = hash(item[j], 0);

            if(keys[0][pos[0]] == item[j]){
                counters[0][pos[0]] += 1;
                if(counters[0][pos[0]] >= 128){
                    coco_ours_que[queue_id].enqueue(Coco_Ours_Entry(item[j], pos[0], pos[1], counters[0][pos[0]]));
                    counters[0][pos[0]] = 0;
                }
                continue;
            }

            if(keys[1][pos[1]] == item[j]){
                counters[1][pos[1]] += 1;
                if(counters[1][pos[1]] >= 128){
                    coco_ours_que[queue_id].enqueue(Coco_Ours_Entry(item[j], pos[0], pos[1], counters[1][pos[1]]));
                    counters[1][pos[1]] = 0;
                }
                continue;
            }

            choice = (counters[0][pos[0]] > counters[1][pos[1]]);
            counters[choice][pos[choice]] += 1;
            if(rng() % counters[choice][pos[choice]] == 0){
                keys[choice][pos[choice]] = item[j];
            }
            if(counters[choice][pos[choice]] >= 128){
                coco_ours_que[queue_id].enqueue(Coco_Ours_Entry(keys[choice][pos[choice]], pos[0], pos[1], counters[choice][pos[choice]]));
                counters[choice][pos[choice]] = 0;
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
coco_ours_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			coco_ours_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			coco_ours_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			coco_ours_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			coco_ours_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			coco_ours_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}
