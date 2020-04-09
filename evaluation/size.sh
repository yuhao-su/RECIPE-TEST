#!/bin/bash


echo "*********ART SIZE WORKLOAD_C UNIFORM*********"

num_thd=1
RUN_SIZE=16000000 
for LOAD_SIZE in 1 2 4 8 16 32 64 128 256 512 1024;
do
    for i in {1..4..1}
    do
        echo "---LOAD_SIZE * $LOAD_SIZE * $i---"
        ../build/ycsb art c randint uniform $num_thd $[LOAD_SIZE*10000] $RUN_SIZE
    done
    echo -e "\n"
done