#include "benchmark.h"

int main(int argc, char *argv[]) {
    std::ios::sync_with_stdio(false);
    Benchmark<uint64_t> bench(argv[1]);
    bench.Bench();
    return 0;
}
