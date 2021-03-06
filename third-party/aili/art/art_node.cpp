/**
 *    author:     UncP
 *    date:    2019-02-06
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <emmintrin.h>

#ifdef Debug
#include <stdio.h>
#endif

#ifdef Allocator
#include "../palm/allocator.h"
#endif

#include "art_node.h"
namespace aili_art
{




static inline void art_node_set_offset(art_node *an, size_t off)
{
  debug_assert(off < 256);
  an->version = set_offset(an->version, off);
}


static inline art_node* _new_art_node(size_t size)
{
  #ifdef Allocator
  art_node *an = (art_node *)allocator_alloc(size);
  #else
  art_node *an = (art_node *)malloc(size);
  #endif
  an->version = 0;
  an->_new = 0;
  an->parent = 0;
  return an;
}

static inline art_node* new_art_node4()
{
  art_node *an = _new_art_node(sizeof(art_node4));
  an->version = set_type(an->version, node4);
  return an;
}

static inline art_node* new_art_node16()
{
  art_node *an = _new_art_node(sizeof(art_node16));
  an->version = set_type(an->version, node16);
  return an;
}

static inline art_node* new_art_node48()
{
  art_node *an = _new_art_node(sizeof(art_node48));
  an->version = set_type(an->version, node48);
  memset(((art_node48 *)an)->index, 0, 256);
  return an;
}

static inline art_node* new_art_node256()
{
  art_node *an = _new_art_node(sizeof(art_node256));
  memset(an, 0, sizeof(art_node256));
  an->version = set_type(an->version, node256);
  return an;
}

art_node* new_art_node()
{
  return new_art_node4();
}

void free_art_node(art_node *an)
{
  #ifdef Allocator
  (void)an;
  #else
  free((void *)an);
  #endif
}

art_node** art_node_find_child(art_node *an, uint64_t version, unsigned char byte)
{
  debug_assert(is_leaf(an) == 0);

  switch (get_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4*)an;
    debug_assert(get_count(version) < 5);
    for (int i = 0, count = get_count(version); i < count; ++i)
      if (an4->key[i] == byte) {
        debug_assert(an4->child[i]);
        return &(an4->child[i]);
      }
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    debug_assert(get_count(version) < 17);
    // for (int i = 0, count = get_count(version); i < count; ++i)
    //  if (an16->key[i] == byte) {
    //    debug_assert(an16->child[i]);
    //    return &(an16->child[i]);
    //  }
    __m128i key = _mm_set1_epi8(byte);
    __m128i key2 = _mm_loadu_si128((__m128i *)an16->key);
    __m128i cmp = _mm_cmpeq_epi8(key, key2);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    if (bitfield) {
      debug_assert(an16->child[__builtin_ctz(bitfield)]);
      return &(an16->child[__builtin_ctz(bitfield)]);
    }
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    debug_assert(get_count(version) < 49);
    int index = an48->index[byte];
    if (index) {
      debug_assert(an48->child[index - 1]);
      return &(an48->child[index - 1]);
    }
  }
  break;
  case node256: {
    art_node256 *an256 = (art_node256 *)an;
    if (an256->child[byte])
      return &(an256->child[byte]);
  }
  break;
  default:
    assert(0);
  }
  return 0;
}

static inline void art_node_set_new_node(art_node *old, art_node *_new)
{
  __atomic_store(&old->_new, &_new, __ATOMIC_RELAXED);
}

static inline art_node* art_node_get_new_node(art_node *old)
{
  art_node *_new;
  __atomic_load(&old->_new, &_new, __ATOMIC_RELAXED);
  return _new;
}

// require: node is locked
static art_node* art_node_grow(art_node *an)
{
  art_node *_new;
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  switch (get_type(version)) {
  case node4: {
    art_node16 *an16 = (art_node16 *)(_new = new_art_node16());
    art_node4 *an4 = (art_node4 *)an;
    debug_assert(get_count(version) == 4);
    memcpy(an16->prefix, an4->prefix, 8);
    an16->version = set_prefix_len(an16->version, get_prefix_len(version));
    for (int i = 0; i < 4; ++i) {
      an16->key[i] = an4->key[i];
      an16->child[i] = an4->child[i];
      if (!is_leaf(an4->child[i]))
        an4->child[i]->parent = _new;
    }
    an16->version = set_count(an16->version, 4);
  }
  break;
  case node16: {
    art_node48 *an48 = (art_node48 *)(_new = new_art_node48());
    art_node16 *an16 = (art_node16 *)an;
    debug_assert(get_count(version) == 16);
    memcpy(an48->prefix, an16->prefix, 8);
    an48->version = set_prefix_len(an48->version, get_prefix_len(version));
    for (int i = 0; i < 16; ++i) {
      an48->child[i] = an16->child[i];
      if (!is_leaf(an16->child[i]))
        an16->child[i]->parent = _new;
      an48->index[an16->key[i]] = i + 1;
    }
    an48->version = set_count(an48->version, 16);
  }
  break;
  case node48: {
    art_node256 *an256 = (art_node256 *)(_new = new_art_node256());
    art_node48 *an48 = (art_node48 *)an;
    debug_assert(get_count(version) == 48);
    memcpy(an256->prefix, an48->prefix, 8);
    an256->version = set_prefix_len(an256->version, get_prefix_len(version));
    for (int i = 0; i < 256; ++i) {
      int index = an48->index[i];
      if (index) {
        an256->child[i] = an48->child[index - 1];
        if (!is_leaf(an48->child[index - 1]))
          an48->child[index - 1]->parent = _new;
      }
    }
  }
  break;
  default:
    // node256 is not growable
    assert(0);
  }

  assert(art_node_lock(_new) == 0);
  art_node_set_offset(_new, get_offset(version));
  art_node_set_new_node(an, _new);
  art_node_set_version(an, set_old(version));
  return _new;
}

// add a child to art_node, return 0 on success, otherwise return next layer
// require: node is locked
art_node** art_node_add_child(art_node *an, unsigned char byte, art_node *child, art_node **_new)
{
  debug_assert(is_leaf(an) == 0);

  uint64_t version = an->version;
  debug_assert(is_locked(version));

  art_node **next;
  if ((next = art_node_find_child(an, version, byte)))
    return next;

  // grow if necessary
  if (unlikely(art_node_is_full(an))) {
    *_new = art_node_grow(an);
    an = *_new;
    version = an->version;
  }

  switch (get_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4 *)an;
    debug_assert(get_count(version) < 4);
    for (int i = 0, count = get_count(version); i < count; ++i)
      debug_assert(an4->key[i] != byte);
    // no need to be ordered
    int count = get_count(version);
    an4->child[count] = child;
    an4->key[count] = byte;
    an4->version = incr_count(version);
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    #ifdef Debug
    __m128i key = _mm_set1_epi8(byte);
    __m128i key2 = _mm_loadu_si128((__m128i *)an16->key);
    __m128i cmp = _mm_cmpeq_epi8(key, key2);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    debug_assert(bitfield == 0);
    #endif
    // no need to be ordered
    int count = get_count(version);
    an16->child[count] = child;
    an16->key[count] = byte;
    an16->version = incr_count(version);
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    debug_assert(an48->index[byte] == 0);
    version = incr_count(version);
    an48->child[get_count(version) - 1] = child;
    an48->index[byte] = get_count(version);
    an48->version = version;
  }
  break;
  case node256: {
    art_node256 *an256 = (art_node256 *)an;
    debug_assert(an256->child[byte] == 0);
    an256->child[byte] = child;
  }
  break;
  default:
    assert(0);
  }

  if (_new && *_new)
    art_node_unlock(*_new);
  return 0;
}



void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len)
{
  memcpy(an->prefix, (char *)key + off, prefix_len);
  an->version = set_prefix_len(an->version, prefix_len);
}

// return the first offset that differs
int art_node_prefix_compare(art_node *an, uint64_t version, const void *key, size_t len, size_t off)
{
  debug_assert(off <= len);

  int prefix_len = get_prefix_len(version);
  const char *prefix = an->prefix, *cur = (const char *)key;
  debug_assert(prefix_len >= 0 && prefix_len <= 8);

  int i = 0;
  for (; i < prefix_len && off < len; ++i, ++off) {
    if (prefix[i] != cur[off])
      return i;
  }

  return i;
}

// require: node is locked
unsigned char art_node_truncate_prefix(art_node *an, int off)
{
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  debug_assert(off < get_prefix_len(version));

  // mark expand bit before truncate prefix
  version = set_expand(version);
  art_node_set_version(an, version);

  int prefix_len = get_prefix_len(version);
  char *prefix = an->prefix;
  unsigned char ret = prefix[off];
  for (int i = 0, j = off + 1; j < prefix_len; ++i, ++j)
    prefix[i] = prefix[j];

  version = set_prefix_len(version, prefix_len - off - 1);
  off += get_offset(version) + 1;
  version = set_offset(version, off);
  art_node_set_version_unsafe(an, version);

  return ret;
}



uint64_t art_node_get_stable_expand_version(art_node *an)
{
  int loop = 4;
  uint64_t version = art_node_get_version_unsafe(an);
  while (is_expanding(version)) {
    for (int i = 0; i < loop; ++i)
      __asm__ volatile("pause" ::: "memory");
    if (loop < 128)
      loop += loop;
    version = art_node_get_version_unsafe(an);
  }
  return version;
}

// uint64_t art_node_get_stable_insert_version(art_node *an)
// {
//   uint64_t version;
//   do {
//     version = art_node_get_version(an);
//   } while (is_inserting(version));
//   return version;
// }



// inline int art_node_version_compare_insert(uint64_t version1, uint64_t version2)
// {
//   return is_inserting(version1) != is_inserting(version2) || get_vinsert(version1) != get_vinsert(version2);
// }

// return 0 on success, 1 on failure
int art_node_lock(art_node *an)
{
  while (1) {
    // must use `acquire` operation to avoid deadlock
    uint64_t version = art_node_get_version(an);
    if (is_locked(version)) {
      // __asm__ __volatile__ ("pause");
      continue;
    }
    if (unlikely(is_old(version)))
      return 1;
    if (__atomic_compare_exchange_n(&an->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
  return 0;
}

static inline art_node* art_node_get_parent(art_node *an)
{
  art_node *parent;
  __atomic_load(&an->parent, &parent, __ATOMIC_ACQUIRE);
  return parent;
}



art_node* art_node_get_locked_parent(art_node *an)
{
  art_node *parent;
  while (1) {
    if ((parent = art_node_get_parent(an)) == 0)
      break;
    if (unlikely(art_node_lock(parent)))
      continue;
    if (art_node_get_parent(an) == parent)
      break;
    art_node_unlock(parent);
  }
  return parent;
}

// require: node is locked
void art_node_unlock(art_node *an)
{
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  //if (is_inserting(version)) {
  //  incr_vinsert(version);
  //  version = unset_insert(version);
  //}
  if (is_expanding(version)) {
    version = incr_vexpand(version);
    version = unset_expand(version);
  }

  art_node_set_version(an, unset_lock(version));
}

int art_node_version_is_old(uint64_t version)
{
  return is_old(version);
}

art_node* art_node_replace_leaf_child(art_node *an, const void *key, size_t len, size_t off)
{
  debug_assert(is_leaf(an));

  const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
  size_t l1 = get_leaf_len(an), l2 = len, i;
  for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
    ;
  if (unlikely(i == l1 && i == l2))
    return 0; // key exists

  art_node *_new = new_art_node();
  art_node_set_offset(_new, off);
  assert(art_node_lock(_new) == 0);
  // TODO: i - off might be bigger than 8
  assert(i - off <= 8);
  art_node_set_prefix(_new, k1, off, i - off);
  off = i;
  unsigned char byte;
  byte = off == l1 ? 0 : k1[off];
  assert(art_node_add_child(_new, byte, an, 0) == 0);
  byte = off == l2 ? 0 : k2[off];
  assert(art_node_add_child(_new, byte, (art_node *)make_leaf(k2), 0) == 0);
  art_node_unlock(_new);

  return _new;
}

// require: node is locked
art_node* art_node_expand_and_insert(art_node *an, const void *key, size_t len, size_t off, int common)
{
  debug_assert(is_locked(an->version));

  art_node* _new = new_art_node();
  art_node_set_offset(_new, off);
  assert(art_node_lock(_new) == 0);
  art_node_set_prefix(_new, key, off, common);
  unsigned char byte;
  byte = (off + common < len) ? ((unsigned char *)key)[off + common] : 0;
  assert(art_node_add_child(_new, byte, (art_node *)make_leaf(key), 0) == 0);
  byte = art_node_truncate_prefix(an, common);
  assert(art_node_add_child(_new, byte, an, 0) == 0);
  art_node_unlock(_new);

  return _new;
}

// require: parent is locked
void art_node_replace_child(art_node *parent, unsigned char byte, art_node *old, art_node *_new)
{
  (void)old;
  uint64_t version = parent->version;
  debug_assert(is_locked(version));

  art_node **child = art_node_find_child(parent, version, byte);

  debug_assert(child && *child == old);

  __atomic_store(child, &_new, __ATOMIC_RELEASE);
  _new->parent = parent;
}

#ifdef Debug
void art_node_print(art_node *an)
{
  uint64_t version = art_node_get_version(an);

  if (an->_new) {
    printf("has _new:\n");
    art_node_print(an->_new);
  }

  printf("%p\n", an);
  printf("is_locked:  %u\n", !!is_locked(version));
  printf("is_old:  %u\n", !!is_old(version));
  printf("is_leaf:  %u\n", !!is_leaf(an));
  printf("is_expand:  %u  vexpand:  %u\n", !!is_expanding(version), get_vexpand(version));
  printf("prefix_len: %d\n", get_prefix_len(version));
  printf("children_cnt: %d\n", get_count(version));
  for (int i = 0; i < get_prefix_len(version); ++i) {
    printf("%d ", (unsigned char)an->prefix[i]);
  }
  printf("\n");
  switch (get_type(version)) {
  case node4: {
    printf("type 4\n");
    art_node4 *an4 = (art_node4 *)an;
    for (int i = 0; i < get_count(version); ++i) {
      if (!is_leaf(an4->child[i]))
        printf("%d %p\n", an4->key[i], an4->child[i]);
      else {
        printf("%d ", an4->key[i]);
        print_key(get_leaf_key(an4->child[i]), 8);
      }
    }
  }
  break;
  case node16: {
    printf("type 16\n");
    art_node16 *an16 = (art_node16 *)an;
    for (int i = 0; i < get_count(version); ++i) {
      if (!is_leaf(an16->child[i]))
        printf("%d %p\n", an16->key[i], an16->child[i]);
      else {
        printf("%d ", an16->key[i]);
        print_key(get_leaf_key(an16->child[i]), 8);
      }
    }
  }
  break;
  case node48: {
    printf("type 48\n");
    art_node48 *an48 = (art_node48 *)an;
    for (int i = 0; i < 256; ++i)
      if (an48->index[i]) {
        if (!is_leaf(an48->child[an48->index[i] - 1]))
          printf("%d %p\n", i, an48->child[an48->index[i] - 1]);
        else {
          printf("%d ", i);
          print_key(get_leaf_key(an48->child[an48->index[i] - 1]), 8);
        }
      }
  }
  break;
  case node256: {
    printf("type 256\n");
    art_node256 *an256 = (art_node256 *)an;
    for (int i = 0; i < 256; ++i)
      if (an256->child[i]) {
        if (!is_leaf(an256->child[i]))
          printf("%d %p\n", i, an256->child[i]);
        else {
          printf("%d ", i);
          print_key(get_leaf_key(an256->child[i]), 8);
        }
      }
  }
  break;
  default:
    assert(0);
  }
  printf("\n");
}

void print_key(const void *key, size_t len)
{
  unsigned char *n = (unsigned char *)key;
  for (int i = 0; i < (int)len; ++i) {
    printf("%d ", n[i]);
  }
  printf("\n");
}

#endif // Debug
}