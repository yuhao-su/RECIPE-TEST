#!/bin/bash

LOAD_SIZE=16000000
RUN_SIZE=16000000

# echo "*********ART_ROWEX WORKLOAD_C UNIFORM*********"
# for num_thd in 1 {5..40..5};
# do
#     for i in {1..4..1}
#     do
#         echo "---THREAD * $num_thd * $i---"
#         ../build/ycsb art c randint uniform $num_thd $LOAD_SIZE $RUN_SIZE
#         sleep 1s
#     done
#     echo -e "\n"
# done

echo "*********ART_AMAC WORKLOAD_C UNIFORM*********"
for num_thd in 1 {5..40..5};
do
    for i in {1..4..1}
    do
        echo "---THREAD * $num_thd * $i---"
        ../build/ycsb art_amac c randint uniform $num_thd $LOAD_SIZE $RUN_SIZE
        sleep 1s
    done
    echo -e "\n"
done
