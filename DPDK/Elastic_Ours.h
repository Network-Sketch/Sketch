#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define Elastic_Ours_COUNTER_PER_BUCKET 4
#define Elastic_Ours_LAMBDA 8

#define Elastic_Ours_HASH_NUM 3
#define Elastic_Ours_LENGTH (1 << 16)
#define Elastic_Ours_HEAP_SIZE 0x3ff


struct Elastic_Ours_Bucket{
    int32_t vote;
    Key ID[Elastic_Ours_COUNTER_PER_BUCKET];
    int32_t count[Elastic_Ours_COUNTER_PER_BUCKET];
};

struct Elastic_Ours_Entry{
    Key key;
    uint32_t pos;
    uint16_t value;
    bool bucket;

    Elastic_Ours_Entry(Key _key = 0, uint32_t _pos = 0, uint16_t _value = 0, bool _bucket = false):
            key(_key), pos(_pos), value(_value), bucket(_bucket){};
};

typedef ReaderWriterQueue<Elastic_Ours_Entry> Elastic_Ours_Queue;
static Elastic_Ours_Queue elastic_ours_que[NUM_RX_QUEUE];

static void
elastic_ours_coordinator()
{
	Elastic_Ours_Entry temp;
    const uint32_t sketch_length = Elastic_Ours_HASH_NUM * Elastic_Ours_LENGTH, bucket_length = Elastic_Ours_HEAP_SIZE * 3 / Elastic_Ours_COUNTER_PER_BUCKET;

	int32_t* sketch = new int32_t [sketch_length];
    memset(sketch, 0, sizeof(int32_t) * sketch_length);

    Elastic_Ours_Bucket* bucket = new Elastic_Ours_Bucket [bucket_length];
    memset(bucket, 0, sizeof(Elastic_Ours_Bucket) * bucket_length);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            while(elastic_ours_que[i].try_dequeue(temp)){
                if(temp.bucket){
                    int32_t minVal = 0x7fffffff;
                    uint32_t minPos = 0;

                    for(uint32_t j = 0; j < Elastic_Ours_COUNTER_PER_BUCKET; j++){
                        if(bucket[temp.pos].ID[j] == temp.key) {
                            bucket[temp.pos].count[j] += temp.value;
                            goto Ours_Collect_End0;
                        }

                        if(bucket[temp.pos].count[j] < minVal){
                            minPos = j;
                            minVal = bucket[temp.pos].count[j];
                        }
                    }

                    if((bucket[temp.pos].vote + temp.value) >= minVal * Elastic_Ours_LAMBDA){
                        bucket[temp.pos].vote = 0;

                        if(minVal != 0){
                            uint32_t position = hash(bucket[temp.pos].ID[minPos], 101) % sketch_length;
                            sketch[position] = sketch[position] + bucket[temp.pos].count[minPos];
                        }

                        bucket[temp.pos].ID[minPos] = temp.key;
                        bucket[temp.pos].count[minPos] = temp.value;
                    }
                    else {
                        bucket[temp.pos].vote += temp.value;
                        uint32_t position = hash(temp.key, 101) % sketch_length;
                        sketch[position] = sketch[position] + temp.value;
                    }

                }
                else{
                    sketch[temp.pos] += temp.value;
                }

                Ours_Collect_End0:
                {};
            }
        }
	}
}

/* main processing loop */
static void
elastic_ours_local(unsigned queue_id)
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
    const uint32_t sketch_length = Elastic_Ours_HASH_NUM * Elastic_Ours_LENGTH, bucket_length = Elastic_Ours_HEAP_SIZE * 3 / Elastic_Ours_COUNTER_PER_BUCKET;
	uint16_t* sketch = new uint16_t [sketch_length];
    memset(sketch, 0, sizeof(uint16_t) * sketch_length);

    Elastic_Ours_Bucket* buckets = new Elastic_Ours_Bucket [bucket_length];
    memset(buckets, 0, sizeof(Elastic_Ours_Bucket) * bucket_length);

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

		for (i = 0; i < nb_rx; i++) {
			m = pkts_burst[i];
			rte_prefetch0(rte_pktmbuf_mtod(m, void *));
			rte_pktmbuf_adj(m, (uint16_t)sizeof(struct rte_ether_hdr));	
			struct rte_ipv4_hdr *ip_hdr = rte_pktmbuf_mtod(m, struct rte_ipv4_hdr *);
			
			Key src = rte_be_to_cpu_32(ip_hdr->src_addr);
			Key dst = rte_be_to_cpu_32(ip_hdr->dst_addr);
			item[i] = ((src << 32) | dst);

            rte_pktmbuf_free(m);
        }

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].poll_cycles += (end - start);
		start = rte_get_tsc_cycles();
#endif

        for (i = 0; i < nb_rx; i++) {
            uint32_t pos = hash(item[i]) % bucket_length, minPos = 0;
            int32_t minVal = 0x7fffffff;

            for (uint32_t j = 0; j < Elastic_Ours_COUNTER_PER_BUCKET; j++){
                if(buckets[pos].ID[j] == item[i]) {
                    buckets[pos].count[j] += 1;
                    if(buckets[pos].count[j] >= 128){
                        elastic_ours_que[queue_id].enqueue(Elastic_Ours_Entry(buckets[pos].ID[j], pos, buckets[pos].count[j], true));
                        buckets[pos].count[j] = 0;
                    }
                    goto Elastic_Ours_End;
                }

                if(buckets[pos].count[j] < minVal){
                    minPos = j;
                    minVal = buckets[pos].count[j];
                }
            }

            if((buckets[pos].vote + 1) >= minVal * Elastic_Ours_LAMBDA){
                buckets[pos].vote = 0;

                if(minVal != 0){
                    uint32_t position = hash(buckets[pos].ID[minPos], 101) % sketch_length;
                    sketch[position] = sketch[position] + buckets[pos].count[minPos];

                    if(sketch[position] >= 128){
                        elastic_ours_que[queue_id].enqueue(Elastic_Ours_Entry(item[i], position, sketch[position], false));
                        sketch[position] = 0;
                    }
                }
        
                buckets[pos].ID[minPos] = item[i];
                buckets[pos].count[minPos] = 1;
            }
            else {
                buckets[pos].vote += 1;
                uint32_t position = hash(item[i], 101) % sketch_length;
                sketch[position] = sketch[position] + 1;
                if(sketch[position] >= 128){
                    elastic_ours_que[queue_id].enqueue(Elastic_Ours_Entry(item[i], position, sketch[position], false));
                    sketch[position] = 0;
                }
            }

			
            Elastic_Ours_End:
            {}
		}

#ifdef CYCLES
		end = rte_get_tsc_cycles();
		port_statistics[queue_id].sketch_cycles += (end - start);
#endif
		/* >8 End of read packet from RX queues. */
	}
}

static int
elastic_ours_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			elastic_ours_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			elastic_ours_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			elastic_ours_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			elastic_ours_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			elastic_ours_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}