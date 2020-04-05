#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include "tbb/tbb.h"
#include <stdio.h>
#include <tbb/compat/thread>
using namespace std;

#include "P-ART/Tree.h"

#include "third-party/WOART/woart.h"
#include "third-party/libart/src/art.h"
#include "third-party/CART/cart.h"
#include "third-party/CART/operation.h"
#include "third-party/aili/art/art.h"
// #include "third-party/aili/art/art_node.h"



#include "ssmem.h"

#define MULTI_THREAD



// index types
enum {
    TYPE_ART,
    TYPE_HOT,
    TYPE_BWTREE,
    TYPE_MASSTREE,
    TYPE_CLHT,
    TYPE_FASTFAIR,
    TYPE_LEVELHASH,
    TYPE_CCEH,
    TYPE_WOART,
    TYPE_ART_ST,
    TYPE_CART,
    TYPE_MULTI_ART,
    TYPE_ART_ACMC
};

enum {
    OP_INSERT,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_E,
};

enum {
    RANDINT_KEY,
    STRING_KEY,
};

enum {
    UNIFORM,
    ZIPFIAN,
    SMALL
};

namespace Dummy {
    inline void mfence() {asm volatile("mfence":::"memory");}

    inline void clflush(char *data, int len, bool front, bool back)
    {
        if (front)
            mfence();
        volatile char *ptr = (char *)((unsigned long)data & ~(64 - 1));
        for (; ptr < data+len; ptr += 64){
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        }
        if (back)
            mfence();
    }
}



/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  inline bool operator()(uint64_t k1, uint64_t k2) const {
      return k1 == k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      if (strlen(k1) != strlen(k2))
          return false;
      else
          return memcmp(k1, k2, strlen(k1)) == 0;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};
/////////////////////////////////////////////////////////////////////////////////

////////////////////////Helper functions for P-HOT/////////////////////////////
typedef struct IntKeyVal {
    uint64_t key;
    uintptr_t value;
} IntKeyVal;

template<typename ValueType = IntKeyVal *>
class IntKeyExtractor {
    public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key;
    }
};

template<typename ValueType = Key *>
class KeyExtractor {
    public:
    typedef char const * KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return (char const *)value->fkey;
    }
};

/////////////////////////////////////////////////////////////////////////////////

uint32_t num_thread = 0;
uint64_t cart_read_batch_size = 0;
static const uint64_t LOAD_SIZE = 16000000;
static const uint64_t RUN_SIZE = 16000000;
static uint64_t CART_READ_BATCH_SIZE;
// int a =0xfffff;
static const uint64_t CART_WRITE_BATCH_SIZE = 0xffff;

void loadKey(TID tid, Key &key) {
    return ;
}

void ycsb_load_run_string(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<Key *> &init_keys,
        std::vector<Key *> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloada";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloada";
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadb";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadb";
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadc";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadc";
        } else if (kt == STRING_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadd";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadd";
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloade";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloade";
        }
    } else if (ap == ZIPFIAN){
        if (kt == STRING_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloada";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloada";
        } else if (kt == STRING_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadb";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadb";
        } else if (kt == STRING_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadc";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadc";
        } else if (kt == STRING_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloadd";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloadd";
        } else if (kt == STRING_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads/ycsbkey_load_workloade";
            txn_file = "./index-microbench/workloads/ycsbkey_run_workloade";
        }
    } else if(ap == SMALL) {

    }

    std::ifstream infile_load(init_file);

    std::string op;
    std::string key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string maxKey("z");

    int count = 0;
    uint64_t val;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        val = std::stoul(key.substr(4, key.size()));
        init_keys.push_back(init_keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            val = std::stoul(key.substr(4, key.size()));
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, val));
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(keys[count]->make_leaf((char *)key.c_str(), key.size()+1, 0));
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf((char *)init_keys[i]->fkey, init_keys[i]->key_len, init_keys[i]->value);
                    tree.insert(key, t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Run
            Key *end = end->make_leaf((char *)maxKey.c_str(), maxKey.size()+1, 0);
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key;
                    if (ops[i] == OP_INSERT) {
                        key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        tree.insert(key, t);
                    } else if (ops[i] == OP_READ) {
                        key = key->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        Key *val = reinterpret_cast<Key *>(tree.lookup(key, t));
                        if (val->value != keys[i]->value) {
                            std::cout << "[ART] wrong key read: " << val->value << " expected:" << keys[i]->value << std::endl;
                            throw;
                        }
                    } else if (ops[i] == OP_SCAN) {
                        Key *results[200];
                        Key *continueKey = NULL;
                        size_t resultsFound = 0;
                        size_t resultsSize = ranges[i];
                        Key *start = start->make_leaf((char *)keys[i]->fkey, keys[i]->key_len, keys[i]->value);
                        tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
}  else if (index_type == TYPE_WOART) {
#ifdef STRING_TYPE
        woart_tree *t = (woart_tree *)malloc(sizeof(woart_tree));
        woart_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    woart_insert(t, init_keys[i]->fkey, init_keys[i]->key_len, &init_keys[i]->value);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        woart_insert(t, keys[i]->fkey, keys[i]->key_len, &keys[i]->value);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (woart_search(t, keys[i]->fkey, keys[i]->key_len));
                        if (*ret != keys[i]->value) {
                            printf("[WOART] search key = %lu, search value = %lu\n", keys[i]->value, *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        unsigned long buf[200];
                        woart_scan(t, keys[i]->fkey, keys[i]->key_len, ranges[i], buf);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
#endif
    }
}

void ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file; 
    std::string txn_file;
    std::string work_space_path("/home/syh/code/RECIPE-TEST/");
    if (ap == UNIFORM) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads/loada_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads/loadb_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads/loadc_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads/loadd_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads/loade_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnse_unif_int.dat";
        }
    } else if (ap ==ZIPFIAN) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads_zipf/loada_unif_int.dat";
            txn_file = "./index-microbench/workloads_zipf/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads_zipf/loadb_unif_int.dat";
            txn_file = "./index-microbench/workloads_zipf/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads_zipf/loadc_unif_int.dat";
            txn_file = "./index-microbench/workloads_zipf/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads_zipf/loadd_unif_int.dat";
            txn_file = "./index-microbench/workloads_zipf/txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads_zipf/loade_unif_int.dat";
            txn_file = "./index-microbench/workloads_zipf/txnse_unif_int.dat";
        }
    } else if(ap == SMALL) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads_small/loada_unif_int.dat";
            txn_file = "./index-microbench/workloads_small/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads_small/loadb_unif_int.dat";
            txn_file = "./index-microbench/workloads_small/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads_small/loadc_unif_int.dat";
            txn_file = "./index-microbench/workloads_small/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads_small/loadd_unif_int.dat";
            txn_file = "./index-microbench/workloads_small/txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads_small/loade_unif_int.dat";
            txn_file = "./index-microbench/workloads_small/txnse_unif_int.dat";
        }
    }
    init_file = work_space_path + init_file;
    txn_file = work_space_path + txn_file;


    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            tree.restart_cnt = 0;
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                    tree.insert(key, t);
                }
            });
#else        
            auto t = tree.getThreadInfo();
            for (uint64_t i = 0; i != LOAD_SIZE; i++) {
                Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                tree.insert(key, t);
            }
#endif          
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
            std::cout<<"Restart Count, load "<<tree.restart_cnt<<std::endl;
        }
        sleep(1);
        Key **key = new Key*[RUN_SIZE];
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
            for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                key[i] = key[i]->make_leaf(init_keys[i], sizeof(uint64_t), 0);
            }
        });

        {
            // Run
            tree.restart_cnt = 0;
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                        tree.insert(key, t);
                    } else if (ops[i] == OP_READ) {
                        // Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), 0);
                        uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key[i], t));
                        if (*val != init_keys[i]) {
                            std::cout << "[ART] wrong key read: " << val << " expected:" << keys[i] << std::endl;
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        Key *results[200];
                        Key *continueKey = NULL;
                        size_t resultsFound = 0;
                        size_t resultsSize = ranges[i];
                        Key *start = start->make_leaf(keys[i], sizeof(uint64_t), 0);
                        tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                    }
                }
            });
#else
            auto t = tree.getThreadInfo();
            for (uint64_t i = 0; i != LOAD_SIZE; i++) {
                if (ops[i] == OP_INSERT) {
                    Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                    tree.insert(key, t);
                } else if (ops[i] == OP_READ) {
                    Key *key = key->make_leaf(keys[i], sizeof(uint64_t), 0);
                    uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key, t));
                    // if (*val != keys[i]) {
                    //     std::cout << "[ART] wrong key read: " << val << " expected:" << keys[i] << std::endl;
                    //     exit(1);
                    // }
                } else if (ops[i] == OP_SCAN) {
                    Key *results[200];
                    Key *continueKey = NULL;
                    size_t resultsFound = 0;
                    size_t resultsSize = ranges[i];
                    Key *start = start->make_leaf(keys[i], sizeof(uint64_t), 0);
                    tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                }
            }
#endif           
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
            std::cout<<"Restart Count, run "<<tree.restart_cnt<<std::endl;
        }
        std::cout<<"Tree Size: "<<tree.get_size()<<std::endl;
        std::cout<<"Node Size: "<<tree.restart_cnt<<std::endl;

    } else if (index_type == TYPE_ART_ACMC) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            tree.restart_cnt = 0;
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                    tree.insert(key, t);
                }
            });
            
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
            std::cout<<"Restart Count, load "<<tree.restart_cnt<<std::endl;
        }
        sleep(1);
        Key **key = new Key*[RUN_SIZE];
        void **ret_val = new void*[RUN_SIZE];
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
            for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                key[i] = key[i]->make_leaf(init_keys[i], sizeof(uint64_t), 0);
            }
        });
        {
            // Run
            tree.restart_cnt = 0;
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            uint32_t block_num = num_thread * 16;
            uint32_t _block_size = RUN_SIZE / block_num;
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint32_t>(0, block_num), [&](const tbb::blocked_range<uint32_t> &scope) {

                auto t = tree.getThreadInfo();
                for (uint32_t i = scope.begin(); i != scope.end(); i++) {
                    uint32_t block_size = (i < block_num - 1) ? _block_size : RUN_SIZE - i * _block_size;
                    tree.exec_acmc(key, ret_val, ops, i * _block_size, block_size, t);
                    // if (ops[i] == OP_INSERT) {
                    //     Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                    //     tree.insert(key, t);
                    // } else if (ops[i] == OP_READ) {
                    //     // Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), 0);
                    //     uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key[i], t));
                    //     if (*val != init_keys[i]) {
                    //         std::cout << "[ART] wrong key read: " << val << " expected:" << keys[i] << std::endl;
                    //         exit(1);
                    //     }
                    // } else if (ops[i] == OP_SCAN) {
                    //     Key *results[200];
                    //     Key *continueKey = NULL;
                    //     size_t resultsFound = 0;
                    //     size_t resultsSize = ranges[i];
                    //     Key *start = start->make_leaf(keys[i], sizeof(uint64_t), 0);
                    //     tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                    // }
                }
            });

            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
            std::cout<<"Restart Count, run "<<tree.restart_cnt<<std::endl;
        }

        for (int i = 0; i < RUN_SIZE; i++) {
            if (ret_val[i] == NULL) {
                std::cout<<"key not found: "<<i<<endl;
                // exit(1);
                continue;
            }
            if (*(uint64_t*)ret_val[i] != init_keys[i]) {
                std::cout<<i<<endl;
                std::cout << "[ART] wrong key read: " << *(uint64_t*)ret_val[i] << " expected:" << keys[i] << std::endl;
                exit(1);
            }
        }
        std::cout<<"Tree Size: "<<tree.get_size()<<std::endl;
        std::cout<<"Node Size: "<<tree.restart_cnt<<std::endl;

} else if (index_type == TYPE_WOART) {
#ifndef STRING_TYPE
        woart_tree *t = (woart_tree *)malloc(sizeof(woart_tree));
        woart_tree_init(t);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    woart_insert(t, init_keys[i], sizeof(uint64_t), &init_keys[i]);
                }
            });
#else
            for (uint64_t i = 0; i != LOAD_SIZE; i++) {
                woart_insert(t, init_keys[i], sizeof(uint64_t), &init_keys[i]);
            }
#endif
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    if (ops[i] == OP_INSERT) {
                        woart_insert(t, keys[i], sizeof(uint64_t), &keys[i]);
                    } else if (ops[i] == OP_READ) {
                        uint64_t *ret = reinterpret_cast<uint64_t *> (woart_search(t, keys[i], sizeof(uint64_t)));
                        if (*ret != keys[i]) {
                            printf("[WOART] expected = %lu, search value = %lu\n", keys[i], *ret);
                            exit(1);
                        }
                    } else if (ops[i] == OP_SCAN) {
                        unsigned long buf[200];
                        woart_scan(t, keys[i], ranges[i], buf);
                    }
                }
            });
#else
            for (uint64_t i = 0; i != RUN_SIZE; i++) {
                if (ops[i] == OP_INSERT) {
                    woart_insert(t, keys[i], sizeof(uint64_t), &keys[i]);
                } else if (ops[i] == OP_READ) {
                    uint64_t *ret = reinterpret_cast<uint64_t *> (woart_search(t, keys[i], sizeof(uint64_t)));
                    // if (*ret != keys[i]) {
                    //     printf("[WOART] expected = %lu, search value = %lu\n", keys[i], *ret);
                    //     exit(1);
                    // }
                } else if (ops[i] == OP_SCAN) {
                    unsigned long buf[200];
                    woart_scan(t, keys[i], ranges[i], buf);
                }
            }
#endif
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
#endif
    } else if (index_type == TYPE_ART_ST) {
        art_tree *t = (art_tree*)malloc(sizeof(art_tree));
        art_tree_init(t);
        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            for (uint64_t i = 0; i < LOAD_SIZE; i++) {
                art_insert(t, (unsigned char*)&init_keys[i], 8, &init_keys[i]);
            }
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }
        {
#ifdef ART_BATCH
            
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
            // #pragma omp parallel for
            // for (uint64_t i = 0; i < RUN_SIZE; i++) {
                if (ops[i] == OP_INSERT) {
                    art_insert(t, (unsigned char*)&keys[i], 8, &keys[i]);
                } else if (ops[i] == OP_READ) {
                    uint64_t *ret = reinterpret_cast<uint64_t *> (art_search(t, (unsigned char*)&keys[i], 8));
                    if (*ret != keys[i]) {
                        printf("[ART_ST] expected = %lu, search value = %lu\n", keys[i], *ret);
                        exit(1);
                    }
                } 
            }
            });
#else
            uint64_t batch_cnt = 0;
            uint64_t time_r = 0, time_w = 0, time_sort = 0;
            auto starttime = std::chrono::system_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
            for (uint64_t i = 0; i < RUN_SIZE; i++) {
                if (ops[i] == OP_INSERT || ops[i] == OP_READ) {
                    batch_cnt++;
                }
                if (batch_cnt == CART_READ_BATCH_SIZE || i == RUN_SIZE - 1) {                    
                    sort(keys.begin() + i - batch_cnt + 1, keys.begin() + i + 1);
                    duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
                    time_sort += duration.count();

                    starttime = std::chrono::system_clock::now();
                    tbb::parallel_for(tbb::blocked_range<uint64_t>(i - batch_cnt + 1, i + 1), [&](const tbb::blocked_range<uint64_t> &scope) {
                    for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                        if (ops[i] == OP_INSERT) {
                            art_insert(t, (unsigned char*)&keys[i], 8, &keys[i]);
                        } else if (ops[i] == OP_READ) {
                            uint64_t *ret = reinterpret_cast<uint64_t *> (art_search(t, (unsigned char*)&keys[i], 8));
                            if (*ret != keys[i]) {
                                printf("[ART_ST] expected = %lu, search value = %lu\n", keys[i], *ret);
                                exit(1);
                            }
                        } 
                    }
                    });
                    duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
                    time_r += duration.count();
                }
            }
#endif
            printf("Sort: %lld\tRead: %lld\tWrite: %lld\n", time_sort, time_r, time_w);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / (0 + time_r + time_w));
        }
    } else if (index_type == TYPE_CART) {
        cart_tree *t = (cart_tree*)malloc(sizeof(cart_tree));
        tbb::task_group tg;
        ret_res_queue ret_res_q_r[num_thread];
        for (int i = 0; i < num_thread; i++)
            ret_res_q_r[i].ret_res = (ret_result*)malloc(sizeof(ret_result) * CART_READ_BATCH_SIZE);
        cart_tree_init(t);
        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            for (uint64_t i = 0; i < LOAD_SIZE; i++) {
                cart_insert(t, (unsigned char*)&init_keys[i], 8, &init_keys[i]);
            }
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }
        {
            // Run
            int worker_index = tbb::task_arena::current_thread_index();
            // cout<<"id: "<<worker_index<<endl;
            auto starttime = std::chrono::system_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            op_set_rt osr_r = create_osr(CART_READ_BATCH_SIZE);
            op_set_rt osr_w = create_osr(CART_WRITE_BATCH_SIZE);
            uint64_t time_r = 0, time_w = 0, time_sort = 0;
            for (uint64_t i = 0; i < RUN_SIZE; i++) {
                if (ops[i] == OP_INSERT) {
                    osr_w.ocbs[osr_w.len++] = {CART_WRITE, keys[i], 8, keys[i], osr_w.len};
                } else if (ops[i] == OP_READ) {
                    osr_r.ocbs[osr_r.len++] = {CART_READ, keys[i], 8, keys[i], osr_r.len};
                }
                if (osr_r.len == CART_READ_BATCH_SIZE || i == RUN_SIZE - 1) {

                    
                    // std::cout<<"Read batch start"<<std::endl;
                    starttime = std::chrono::system_clock::now();
                    sort_op_set(osr_r);
                    duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
                    time_sort += duration.count();

                    starttime = std::chrono::system_clock::now();
                    // tg.run([&](){cart_batch_search(t, osr_r, tg);});
                    cart_batch_search(t, osr_r, tg, ret_res_q_r);
                    // poll_op_compele(osr_r);
                    tg.wait();
                    
                    duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
                    time_r += duration.count();

                    osr_r.len = 0;
                }
                if (osr_w.len == CART_WRITE_BATCH_SIZE || i == RUN_SIZE - 1) {
                    // execute
                    // polling
                    osr_w.len = 0;
                }
            }
            printf("Sort: %lld\tRead: %lld\tWrite: %lld\n", time_sort, time_r, time_w);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / (0 + time_r + time_w));
        }
    } else if (index_type == TYPE_MULTI_ART) {
        aili_art::adaptive_radix_tree *tree = aili_art::new_adaptive_radix_tree();

        {
            // Load
            int duplication_cnt=0, put_res; 
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                // for (uint64_t i = 0; i != LOAD_SIZE; i++) {
                    char *key = (char*)malloc(16);
                    key[0] = 8;
                    *(uint64_t *)(key + 1) = init_keys[i]; 
                    put_res = aili_art::adaptive_radix_tree_put(tree, (const void *)(key + 1), 8);
                    if(put_res) duplication_cnt++;
                }
            });
#else        
            
#endif          
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
            printf("Duplication count, %d times\n", duplication_cnt);

        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
#ifdef MULTI_THREAD
            // tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                // for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                for (uint64_t i = 0; i != LOAD_SIZE; i++) {
                
                    if (ops[i] == OP_INSERT) {
                        char *key = (char*)malloc(16);
                        key[0] = 8;
                        *(uint64_t *)(key + 1) = keys[i]; 
                        aili_art::adaptive_radix_tree_put(tree, (const void *)(key + 1), 8);
                    } else if (ops[i] == OP_READ) {
                        uint64_t key = keys[i];
                        void *val = aili_art::adaptive_radix_tree_get(tree, &key, 8);
                        // std::cout<<val<<endl;
                        
                        // if (val == 0) {
                        //     std::cout<<"key: "<<key<<endl;
                        // }
                        if (val != 0 && *(uint64_t*)val != keys[i]) {
                            std::cout << "[MULTI_ART] wrong key read: " << *(uint64_t*)val << " expected:" << keys[i] << std::endl;
                            exit(1);
                        }
                    } 
                }
            // });

#endif          
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    }

}   

int main(int argc, char **argv) {
    if (argc != 7) {
        std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads] [batch size]\n";
        std::cout << "1. index type: art hot bwtree masstree clht\n";
        std::cout << "               fastfair levelhash cceh woart\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. key distribution: randint, string\n";
        std::cout << "4. access pattern: uniform, zipfian\n";
        std::cout << "5. number of threads (integer)\n";
        std::cout << "6. For cart only -- batch_size (integer)\n";
        return 1;
    }

    printf("%s, workload%s, %s, %s, threads %s, batch size %s\n", argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);

    int index_type;
    if (strcmp(argv[1], "art") == 0)
        index_type = TYPE_ART;
    else if (strcmp(argv[1], "hot") == 0) {
#ifdef HOT
        index_type = TYPE_HOT;
#else
        return 1;
#endif
    } else if (strcmp(argv[1], "bwtree") == 0)
        index_type = TYPE_BWTREE;
    else if (strcmp(argv[1], "masstree") == 0)
        index_type = TYPE_MASSTREE;
    else if (strcmp(argv[1], "clht") == 0)
        index_type = TYPE_CLHT;
    else if (strcmp(argv[1], "fastfair") == 0)
        index_type = TYPE_FASTFAIR;
    else if (strcmp(argv[1], "levelhash") == 0)
        index_type = TYPE_LEVELHASH;
    else if (strcmp(argv[1], "cceh") == 0)
        index_type = TYPE_CCEH;
    else if (strcmp(argv[1], "woart") == 0)
        index_type = TYPE_WOART;
    else if (strcmp(argv[1], "art_st") == 0)
        index_type = TYPE_ART_ST;
    else if (strcmp(argv[1], "cart") == 0)
        index_type = TYPE_CART;
    else if (strcmp(argv[1], "multi_art") == 0)
        index_type = TYPE_MULTI_ART;
    else if (strcmp(argv[1], "art_acmc") == 0)
        index_type = TYPE_ART_ACMC;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int kt;
    if (strcmp(argv[3], "randint") == 0) {
        kt = RANDINT_KEY;
    } else if (strcmp(argv[3], "string") == 0) {
        kt = STRING_KEY;
    } else {
        fprintf(stderr, "Unknown key type: %s\n", argv[3]);
        exit(1);
    }

    int ap;
    if (strcmp(argv[4], "uniform") == 0) {
        ap = UNIFORM;
    } else if (strcmp(argv[4], "zipfian") == 0) {
        ap = ZIPFIAN;
    } else if (strcmp(argv[4], "small") == 0) {
        ap = SMALL;
    } else {
        fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
        exit(1);
    }

    num_thread = atoi(argv[5]);

    cart_read_batch_size = atoi(argv[6]);
    CART_READ_BATCH_SIZE = cart_read_batch_size;
    tbb::task_scheduler_init init(num_thread);

    if (kt != STRING_KEY) {
        std::vector<uint64_t> init_keys;
        std::vector<uint64_t> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    } else {
        std::vector<Key *> init_keys;
        std::vector<Key *> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(Key *));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(Key *));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_string(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    }

    return 0;
}
