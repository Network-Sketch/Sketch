#include "common.h"

#include "common/hash.h"
#include "common/Heap.h"
#include "queue/readerwriterqueue.h"

#include <thread>
#include <cstring>
#include <pthread.h>

#define Elastic_Merge_COUNTER_PER_BUCKET 4
#define Elastic_Merge_LAMBDA 8

#define Elastic_Merge_HASH_NUM 3
#define Elastic_Merge_LENGTH (1 << 16)
#define Elastic_Merge_HEAP_SIZE 0x3ff


struct Elastic_Merge_Bucket{
    int32_t vote;
    Key ID[Elastic_Merge_COUNTER_PER_BUCKET];
    int32_t count[Elastic_Merge_COUNTER_PER_BUCKET];
};

struct Elastic_Merge_Entry{
    int32_t* sketch;
    Elastic_Merge_Bucket* buckets;

    Elastic_Merge_Entry(int32_t* _sketch = nullptr, Elastic_Merge_Bucket* _buckets = nullptr):
            sketch(_sketch), buckets(_buckets){};
};

typedef ReaderWriterQueue<Elastic_Merge_Entry> Elastic_Merge_Queue;
static Elastic_Merge_Queue elastic_merge_que[NUM_RX_QUEUE];

static void
elastic_merge_coordinator()
{
	Elastic_Merge_Entry temp;
    const uint32_t sketch_length = Elastic_Merge_HASH_NUM * Elastic_Merge_LENGTH, bucket_length = Elastic_Merge_HEAP_SIZE * 3 / Elastic_Merge_COUNTER_PER_BUCKET;

	int32_t* sketch = new int32_t [sketch_length];
    memset(sketch, 0, sizeof(int32_t) * sketch_length);

    Elastic_Merge_Bucket* buckets = new Elastic_Merge_Bucket [bucket_length];
    memset(buckets, 0, sizeof(Elastic_Merge_Bucket) * bucket_length);

	while(true){
		for(uint32_t i = 0;i < NUM_RX_QUEUE;++i){
            if(elastic_merge_que[i].try_dequeue(temp)){
                
                for(uint32_t index = 0;index < sketch_length;++index){
                    sketch[index] += temp.sketch[index];
                }
                delete [] temp.sketch;

                for(uint32_t index = 0;index < bucket_length;++index){
                    for(uint32_t j = 0;j < Elastic_Merge_COUNTER_PER_BUCKET;++j){
                        int32_t minVal = 0x7fffffff;
                        uint32_t minPos = 0;

                        for (uint32_t k = 0; k < Elastic_Merge_COUNTER_PER_BUCKET; k++){
                            if(buckets[index].ID[k] == temp.buckets[index].ID[j]){
                                buckets[index].count[k] += temp.buckets[index].count[j];
                                goto Elastic_Merge_Collect_End;
                            }

                            if(buckets[index].count[k] < minVal){
                                minPos = k;
                                minVal = buckets[index].count[k];
                            }
                        }

                        if((buckets[index].vote + temp.buckets[index].count[j]) >= minVal * Elastic_Merge_LAMBDA){
                            buckets[index].vote = 0;

                            if(minVal != 0){
                                uint32_t position = hash(buckets[index].ID[minPos], 101) % sketch_length;
                                sketch[position] = sketch[position] + buckets[index].count[minPos];
                            }

                            buckets[index].ID[minPos] = temp.buckets[index].ID[j];
                            buckets[index].count[minPos] = temp.buckets[index].count[j];
                        }
                        else {
                            buckets[index].vote += temp.buckets[index].count[j];
                            uint32_t position = hash(temp.buckets[index].ID[j], 101) % sketch_length;
                            sketch[position] = sketch[position] + temp.buckets[index].count[j];
                        }

                        Elastic_Merge_Collect_End:
                        {}
                    }
                }

                delete [] temp.buckets;
            }
        }
	}
}

/* main processing loop */
static void
elastic_merge_local(unsigned queue_id)
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
    const uint32_t sketch_length = Elastic_Merge_HASH_NUM * Elastic_Merge_LENGTH, bucket_length = Elastic_Merge_HEAP_SIZE * 3 / Elastic_Merge_COUNTER_PER_BUCKET;
	int32_t* sketch = new int32_t [sketch_length];
    memset(sketch, 0, sizeof(int32_t) * sketch_length);

    Elastic_Merge_Bucket* buckets = new Elastic_Merge_Bucket [bucket_length];
    memset(buckets, 0, sizeof(Elastic_Merge_Bucket) * bucket_length);

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

            for (uint32_t j = 0; j < Elastic_Merge_COUNTER_PER_BUCKET; j++){
                if(buckets[pos].ID[j] == item[i]) {
                    buckets[pos].count[j] += 1;
                    goto Elastic_Merge_End;
                }

                if(buckets[pos].count[j] < minVal){
                    minPos = j;
                    minVal = buckets[pos].count[j];
                }
            }

            if((buckets[pos].vote + 1) >= minVal * Elastic_Merge_LAMBDA){
                buckets[pos].vote = 0;

                if(minVal != 0){
                    uint32_t position = hash(buckets[pos].ID[minPos], 101) % sketch_length;
                    sketch[position] = sketch[position] + buckets[pos].count[minPos];
                }
        
                buckets[pos].ID[minPos] = item[i];
                buckets[pos].count[minPos] = 1;
            }
            else {
                buckets[pos].vote += 1;
                uint32_t position = hash(item[i], 101) % sketch_length;
                sketch[position] = sketch[position] + 1;
            }

			
            Elastic_Merge_End:
            if((number % (20000 * NUM_RX_QUEUE)) == 0){
                elastic_merge_que[queue_id].enqueue(Elastic_Merge_Entry(sketch, buckets));
                sketch = new int32_t [sketch_length];
                memset(sketch, 0, sizeof(int32_t) * sketch_length);

                buckets = new Elastic_Merge_Bucket [bucket_length];
                memset(buckets, 0, sizeof(Elastic_Merge_Bucket) * bucket_length);
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
elastic_merge_one_lcore(__rte_unused void *dummy)
{
	unsigned lcore_id = rte_lcore_id(), main_id = rte_get_main_lcore();

	if(main_id > NUM_RX_QUEUE){
		if(lcore_id < NUM_RX_QUEUE)
			elastic_merge_local(lcore_id);
		else if(lcore_id == NUM_RX_QUEUE)
			elastic_merge_coordinator();
		else if(lcore_id == main_id)
			print_stats();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}
	else{
		if(lcore_id < main_id)
			elastic_merge_local(lcore_id);
		else if(lcore_id == main_id)
			print_stats();
		else if(lcore_id <= NUM_RX_QUEUE)
			elastic_merge_local(lcore_id - 1);
		else if(lcore_id == NUM_RX_QUEUE + 1)
			elastic_merge_coordinator();
		else
			RTE_LOG(INFO, L2FWD, "nothing to do for lcore %u\n", lcore_id);
	}

	return 0;
}