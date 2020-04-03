/**
 *    author:     UncP
 *    date:    2019-02-05
 *    license:    BSD-3
**/

#ifndef _art_node_h_
#define _art_node_h_
// #define Debug
#include <stddef.h>
#include <stdio.h>
#define node4    ((uint64_t)0x000000)
#define node16   ((uint64_t)0x100000)
#define node48   ((uint64_t)0x200000)
#define node256  ((uint64_t)0x300000)

/**
 *   node version layout(64 bits)
 *       off               count    prefix_len              type  old  lock insert expand vinsert   vexpand
 *   |    8    |   8    |    8     |    8    |     10     |  2  |  1  |  1  |  1  |  1  |    8    |    8    |
 *
**/
#define OLD_BIT    ((uint64_t)1 << 19)
#define LOCK_BIT   ((uint64_t)1 << 18)
#define INSERT_BIT ((uint64_t)1 << 17)
#define EXPAND_BIT ((uint64_t)1 << 16)

#define get_offset(version)          (size_t)(((version) >> 54) & 0xff)
#define set_offset(version, offset)  (((version) & (~(((uint64_t)0xff) << 54))) | ((uint64_t)(offset) << 54))
#define get_prefix_len(version)      (int)(((version) >> 32) & 0xff)
#define set_prefix_len(version, len) (((version) & (~(((uint64_t)0xff) << 32))) | (((uint64_t)(len)) << 32))
#define get_count(version)           (int)(((version) >> 40) & 0xff)
#define set_count(version, count)    (((version) & (~(((uint64_t)0xff) << 40))) | (((uint64_t)(count)) << 40))
#define incr_count(version)          ((version) + ((uint64_t)1 << 40))
#define get_type(version)            (int)((version) & node256)
#define set_type(version, type)      ((version) | type)

#define is_old(version)       ((version) & OLD_BIT)
#define is_locked(version)    ((version) & LOCK_BIT)
#define is_inserting(version) ((version) & INSERT_BIT)
#define is_expanding(version) ((version) & EXPAND_BIT)

#define set_old(version)    ((version) | OLD_BIT)
#define set_lock(version)   ((version) | LOCK_BIT)
#define set_insert(version) ((version) | INSERT_BIT)
#define set_expand(version) ((version) | EXPAND_BIT)

#define unset_lock(version)   ((version) & (~LOCK_BIT))
#define unset_insert(version) ((version) & (~INSERT_BIT))
#define unset_expand(version) ((version) & (~EXPAND_BIT))

#define get_vinsert(version)  ((int)(((version) >> 8) & 0xff))
#define incr_vinsert(version) (((version) & (~((uint64_t)0xff << 8))) | (((version) + (1 << 8)) & (0xff << 8))) // overflow is handled

#define get_vexpand(version)  ((int)((version) & 0xff))
#define incr_vexpand(version) (((version) & ~((uint64_t)0xff)) | (((version) + 1) & 0xff)) // overflow is handled


#ifdef Debug
#include <assert.h>
#define debug_assert(v) assert(v)
#else
#define debug_assert(v)
#endif // Debug

#define fuck printf("fuck\n");
namespace aili_art
{
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define art_node_header \
  uint64_t version;     \
  char prefix[8];       \
  art_node *_new;        \
  art_node *parent;

struct art_node
{
  art_node_header;
};

typedef struct art_node4
{
  art_node_header;
  unsigned char key[4];
  unsigned char unused[4];
  art_node *child[4];
  char meta[0];
}art_node4;

typedef struct art_node16
{
  art_node_header;
  unsigned char key[16];
  aili_art::art_node *child[16];
  char meta[0];
}art_node16;

typedef struct art_node48
{
  art_node_header;
  unsigned char index[256];
  art_node *child[48];
  char meta[0];
}art_node48;

typedef struct art_node256
{
  uint64_t version;
  char prefix[8];
  art_node *_new;
  art_node *parent;
  art_node *child[256];
  char meta[0];
}art_node256;


struct art_node;

inline uintptr_t is_leaf(void* ptr) {
    return (uintptr_t)(ptr) & 1;
}

inline uintptr_t make_leaf(const void *ptr) {
    return (uintptr_t)((const char *)(ptr) - 1) | 1;
}
inline const char* get_leaf_key(void* ptr) {
    return ((const char *)((uintptr_t)(ptr) & (~(uintptr_t)1))) + 1;
}
inline size_t get_leaf_len(void* ptr) {
    return (size_t)*(char *)((uintptr_t)(ptr) & (~(uintptr_t)1));
}

art_node* new_art_node();
void free_art_node(art_node *an);
art_node** art_node_add_child(art_node *an, unsigned char byte, art_node *child, art_node **_new);
art_node** art_node_find_child(art_node *an, uint64_t version, unsigned char byte);
int art_node_is_full(art_node *an);
void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len);
const char* art_node_get_prefix(art_node *an);
int art_node_prefix_compare(art_node *an, uint64_t version, const void *key, size_t len, size_t off);
unsigned char art_node_truncate_prefix(art_node *an, int off);
uint64_t art_node_get_version(art_node *an);
uint64_t art_node_get_version_unsafe(art_node *an);
uint64_t art_node_get_stable_expand_version(art_node *an);
// uint64_t art_node_get_stable_insert_version(art_node *an);
int art_node_version_get_prefix_len(uint64_t version);
int art_node_version_compare_expand(uint64_t version1, uint64_t version2);
// int art_node_version_compare_insert(uint64_t version1, uint64_t version2);
int art_node_lock(art_node *an);
art_node* art_node_get_locked_parent(art_node *an);
void art_node_set_parent_unsafe(art_node *an, art_node *parent);
void art_node_unlock(art_node *an);
int art_node_version_is_old(uint64_t version);
art_node* art_node_replace_leaf_child(art_node *an, const void *key, size_t len, size_t off);
void art_node_replace_child(art_node *parent, unsigned char byte, art_node *old, art_node *_new);
art_node* art_node_expand_and_insert(art_node *an, const void *key, size_t len, size_t off, int common);
inline size_t art_node_version_get_offset(uint64_t version);

inline uint64_t art_node_get_version(art_node *an)
{
  uint64_t version;
  __atomic_load(&an->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

inline void art_node_set_version(art_node *an, uint64_t version)
{
  __atomic_store(&an->version, &version, __ATOMIC_RELEASE);
}

inline uint64_t art_node_get_version_unsafe(art_node *an)
{
  return an->version;
}

inline void art_node_set_version_unsafe(art_node *an, uint64_t version)
{
  an->version = version;
}

inline size_t art_node_version_get_offset(uint64_t version)
{
  return get_offset(version);
}
inline int art_node_version_compare_expand(uint64_t version1, uint64_t version2)
{
  return (is_expanding(version1) != is_expanding(version2)) || (get_vexpand(version1) != get_vexpand(version2));
}
inline void art_node_set_parent_unsafe(art_node *an, art_node *parent)
{
  an->parent = parent;
}
inline int art_node_version_get_prefix_len(uint64_t version)
{
  return get_prefix_len(version);
}
// require: node is locked
inline int art_node_is_full(art_node *an)
{
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  switch (get_type(version)) {
  case node4 : return get_count(version) == 4;
  case node16: return get_count(version) == 16;
  case node48: return get_count(version) == 48;
  default: return 0;
  }
}
#ifdef Debug
void art_node_print(art_node *an);
void print_key(const void *key, size_t len);
#endif
} // namespace aili_art

#endif /* _art_node_h_ */
