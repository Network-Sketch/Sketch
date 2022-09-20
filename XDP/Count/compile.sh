MY_SKETCH=Ours

clang -O2 -g -Wall -target bpf -c $MY_SKETCH-XDP.c -o prog.o
g++ $MY_SKETCH-main.c -o main -lbpf -lxdp

sudo /local/xdp-tools/xdp-loader/xdp-loader unload -a enp65s0f0np0