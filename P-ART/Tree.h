//
// Created by florian on 18.11.15.
//

#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"
#include <atomic>
#include "HashTable.hpp"
#include "AMAC.hpp"
using namespace ART;

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
namespace ART_ROWEX {

    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

        std::atomic<int> restart_cnt;
        int N4_num = 0;
        int N16_num = 0;
        int N48_num = 0;
        int N256_num = 0;
        int leaf_num = 0;


    private:
        N *const root;

        void *checkKey(const Key *ret, const Key *k) const;

        LoadKeyFunction loadKey;

        HashTableUnsafe ht{256*256*256};

        Epoche epoche{256};

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
            SkippedLevel
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
            SkippedLevel
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch,
            SkippedLevel
        };
        CheckPrefixResult checkPrefix(N* n, const Key *k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key *k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey);

        static PCCompareResults checkPrefixCompare(const N* n, const Key *k, uint32_t &level, LoadKeyFunction loadKey);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key *start, const Key *end, LoadKeyFunction loadKey);

    public:

        Tree(LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        ThreadInfo getThreadInfo();

        void *lookup(const Key *k, ThreadInfo &threadEpocheInfo);

        void exec_acmc(Key **key, void** ret_val, std::vector<int> &ops, uint64_t offset, uint64_t len, ThreadInfo &threadEpocheInfo);

        bool lookupRange(const Key *start, const Key *end, const Key *continueKey, Key *result[], std::size_t resultLen,
                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

        void insert(const Key *k, ThreadInfo &epocheInfo);

        void remove(const Key *k, ThreadInfo &epocheInfo);

        void dfs(N* node);

        void get_size();
    };
}
#endif //ART_ROWEX_TREE_H
