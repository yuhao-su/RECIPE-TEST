#include <stdint.h>
#include <iostream>
#include "operation.h"
#include <tbb/tbb.h>
#ifndef CART_H
#define CART_H

#ifdef __cplusplus
extern "C" {
#endif

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define MAX_PREFIX_LEN 8
#define PARALLEL_THRESHOLD 1024

#define CART_ZIPF
#ifndef CART_ZIPF
#define CART_UNIFORM
#endif

#define CART_RET_0
#define CART_SPEC_OPT


extern uint32_t num_thread;
extern uint64_t cart_read_batch_size;

/**
 * Operations in CART 
 */
enum operation_types {CART_READ, CART_WRITE, CART_SCAN};

/**
 * Operation Control block
 */
struct op_ctrl_blk
{
    operation_types op_type;
    uint64_t key;
    uint32_t key_len;
    uint64_t val;
#ifdef CART_RET_0
    volatile uint64_t ret_val;
    volatile bool invalid;
    volatile bool completed; 
#else
    uint32_t id;
#endif
    // uint64_t placeholder1, placeholder2;
    bool operator<(const op_ctrl_blk &ocb) const {
        if (memcmp((uint8_t*)(&(this->key)), ((uint8_t*)&(ocb.key)), ocb.key_len) < 0)
            return true;
        else
            return false;   
    }
};

struct ret_result
{
    uint64_t ret_val;
    bool invalid;
};

struct ret_res_queue
{
    ret_result* ret_res;
    int len;
};




#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*cart_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint32_t partial_len;
    unsigned char partial[MAX_PREFIX_LEN];
} cart_node;

/**
 * Small node with only 4 children
 */
typedef struct {
    cart_node n;
    unsigned char keys[4];
    cart_node *children[4];
} cart_node4;

/**
 * Node with 16 children
 */
typedef struct {
    cart_node n;
    unsigned char keys[16];
    cart_node *children[16];
} cart_node16;

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct {
    cart_node n;
    unsigned char keys[256];
    cart_node *children[48];
} cart_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    cart_node n;
    cart_node *children[256];
} cart_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;
    unsigned char key[];
} cart_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    cart_node *root;
    uint64_t size;
} cart_tree;

/**
 * Runtime operation set
 */
struct op_set_rt
{
    int depth;
    op_ctrl_blk *ocbs;
    int max_len;
    int len;
    cart_node *node;
};

/**
 * Initializes an cart tree
 * @return 0 on success.
 */
int cart_tree_init(cart_tree *t);

/**
 * DEPRECATED
 * Initializes an cart tree
 * @return 0 on success.
 */
#define init_cart_tree(...) cart_tree_init(__VA_ARGS__)

/**
 * Destroys an cart tree
 * @return 0 on success.
 */
int cart_tree_destroy(cart_tree *t);

/**
 * DEPRECATED
 * Initializes an cart tree
 * @return 0 on success.
 */
#define destroy_cart_tree(...) cart_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the cart tree.
 */
#ifdef BROKEN_GCC_C99_INLINE
# define cart_size(t) ((t)->size)
#else
inline uint64_t cart_size(cart_tree *t) {
    return t->size;
}
#endif

/**
 * Inserts a new value into the cart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* cart_insert(cart_tree *t, const unsigned char *key, int key_len, void *value);

/**
 * Deletes a value from the cart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* cart_delete(cart_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the cart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* cart_search(const cart_tree *t, const unsigned char *key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
cart_leaf* cart_minimum(cart_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
cart_leaf* cart_maximum(cart_tree *t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int cart_iter(cart_tree *t, cart_callback cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int cart_iter_prefix(cart_tree *t, const unsigned char *prefix, int prefix_len, cart_callback cb, void *data);

/**
 * Batch search the ART
 * @arg The tree to search
 * @arg The operation set to execute 
*/
void cart_batch_search(const cart_tree* t, op_set_rt osr, tbb::task_group &tg, ret_res_queue* ret_res_q);


#ifdef __cplusplus
}
#endif

/**
 * Sort the operation ctrl block
 * in a operation set
 * @arg operation set need sort
 */
void sort_op_set(op_set_rt);

/**
 * Create a operation set
 * @return a created operation set
 */
op_set_rt create_osr(int);

/**
 * Initiated a operation set from an
 * existed one
 * @arg The osr trying to initiate
 * @arg The operation control blocks
 * @arg The lenth of operation control blocks
 * @arg The depth
 */
void init_osr(op_set_rt* osr, op_ctrl_blk* ocbs, int len, int depth, cart_node* node=NULL);

/**
 * Add a ocb in the osr
 * @arg op set runtime
 * @arg op control block
 */
void add_osr(op_set_rt, op_ctrl_blk);

/**
 * Poll the operation set until all
 * ops are compeleted
 */
void poll_op_compele(op_set_rt);
#endif
