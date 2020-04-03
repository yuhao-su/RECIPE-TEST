#include <operation.h>
#include <cart.h>

#include <tbb/parallel_sort.h>
#include <parallel/algorithm>
void sort_op_set(op_set_rt osr)
{
    // tbb::parallel_sort(osr.ocbs, osr.ocbs + osr.len);
    __gnu_parallel::sort(osr.ocbs, osr.ocbs + osr.len);
}

op_set_rt create_osr(int max_len)
{
    op_set_rt osr;
    osr.depth = 0;
    osr.max_len = max_len;
    osr.len = 0;
    /*
    osr.ocbs = (op_ctrl_blk*)aligned_alloc(64, max_len * sizeof(op_ctrl_blk));
    memset(osr.ocbs, 0, max_len * sizeof(op_ctrl_blk));
    */
    osr.ocbs = (op_ctrl_blk*)calloc(max_len, sizeof(op_ctrl_blk));
    return osr;
}

void init_osr(op_set_rt* osr, op_ctrl_blk* ocbs, int len, int depth, cart_node *node)
{
    osr->depth = depth;
    osr->max_len = len;
    osr->len = len;
    osr->ocbs = ocbs;
    osr->node = node;
}


void add_osr(op_set_rt osr, op_ctrl_blk ocb)
{
    osr.ocbs[osr.len++] = ocb;
}

void poll_op_compele(op_set_rt osr)
{
#ifdef CART_RET_0
    uint64_t completed_cnt = 0;
    while(completed_cnt < osr.len) {
        for (uint64_t i = 0; i < osr.len; i++) {
            op_ctrl_blk ocb = osr.ocbs[i];
            if (ocb.completed) {
                if (ocb.invalid)
                    printf("[CART] None exist value = %lu\n", ocb.key);
                if (ocb.op_type == CART_READ) {
                    if (ocb.ret_val == ocb.key) {
                    ;
                    } else {
                        printf("[CART] expected = %lu, search value = %lu\n", ocb.key, ocb.ret_val);
                        exit(1);
                    }
                }
                completed_cnt++;
            }
        }
        // printf("%lld ", completed_cnt);
    }
#else
#endif
}
