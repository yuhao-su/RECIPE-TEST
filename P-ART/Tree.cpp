#include <assert.h>
#include <functional>
#include <algorithm>
#include "Tree.h"
#include <mmintrin.h>
#include "N.cpp"
#include "Epoche.cpp"
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <fstream>

#ifdef ARTDEBUG
	std::ostream &art_cout = std::cout;
#else
	std::ofstream dev_null("/dev/null");
	std::ostream &art_cout = dev_null;
#endif

namespace ART_ROWEX {

    Tree::Tree(LoadKeyFunction loadKey) : root(new N256(0, {})), loadKey(loadKey) {
        // printf("%d %d %d %d\n", sizeof(N), sizeof(N16), sizeof(N48), sizeof(N256));
        // N::clflush((char *)root, sizeof(N256), true, true);
    }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo() {
        return ThreadInfo(this->epoche);
    }

    void Tree::dfs(N* node) {
        if(N::isLeaf(node)) {
            leaf_num++;
            // restart_cnt++;
            return;
        }
        else {
            switch (node->getType()) {
                case NTypes::N4: {
                    N4_num++;
                    break;
                }
                case NTypes::N16: {
                    N16_num++;
                    break;
                }
                case NTypes::N48: {
                    N48_num++;
                    break;
                }
                case NTypes::N256: {
                    N256_num++;
                    break;
                }
            }
            std::tuple<uint8_t, N *> children[256];
            uint32_t children_cnt;
            node->getChildren(node, 0U, 0xffU, children, children_cnt);
            for (uint32_t i = 0; i < children_cnt; i++) {
                dfs(std::get<1>(children[i]));
            }
        }
    }

    void Tree::get_size() {
        printf("N: %d, N4: %d, N16: %d, N48: %d, N256: %d\n", 
            sizeof(N), sizeof(N4), sizeof(N16), sizeof(N48), sizeof(N256));
        dfs(root);
    }
    void *Tree::lookup(const Key *k, ThreadInfo &threadEpocheInfo) {
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);

        N *node = root;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;
        
        uint64_t cached_key = *(uint64_t*)(&k->fkey[0]) & 0xffffffUL;
        // _mm_prefetch(node, _MM_HINT_T0);
        // N *cached_node = (N*)ht.find(cached_key);
        // if (cached_node) {
        //     // printf("cached key %x\n", cached_key);
        //     // restart_cnt++;
        //     node = cached_node;
        //     level = 3;
        // }
	//art_cout << "Searching key " << k->fkey << std::endl;
        while (true) {
            // if(unlikely(level == 3 && !cached_node)){
            //     uint64_t cached_key = *(uint64_t*)(&k->fkey[0]) & 0xffffffUL;
            //     // printf("level 4 key %x\n", cached_key);
            //     ht.insert(cached_key, node);
            //     // node->cached = 1;
            //     // restart_cnt++;
            // }
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    return NULL;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match: {
                    if (k->getKeyLen() <= level) {
                        return NULL;
                    }
                    node = N::getChild(k->fkey[level], node);
                    
                    if (node == nullptr) {
                        return NULL;
                    }
                    
                    if (N::isLeaf(node)) {
                        Key *ret = N::getLeaf(node);
                        if (level < k->getKeyLen() - 1 || optimisticPrefixMatch) {
                            // restart_cnt++;
                            return checkKey(ret, k);
                        } else {
                            return &ret->value;
                        }
                    }
                }
            }
            level++;
        }
    }
    static inline void return_val(State &state, uint64_t &num_finished, uint64_t &y, 
        const uint64_t offset, void **ret_val, void* val) {
        state.stage = INIT;
        ret_val[state.id] = val;
        y++;
        num_finished++;
    }
    void Tree::exec_acmc(Key **key, void **ret_val, std::vector<int> &ops, 
        uint64_t offset, uint64_t len, ThreadInfo &threadEpocheInfo) {
        uint64_t num_finished = 0, x = 0, y = 0;
        uint32_t group_size = 30;
        Group buff(group_size);
        while (num_finished < len) {
            State &state = buff.next_state();
            // std::cout<<num_finished<<std::endl;
            
            if (state.stage == INIT && x < len) {
                state.key   = key[offset + x];
                state.level = 0;
                state.id    = offset + x;
                if (ops[offset + x] == OP_READ) {
                    state.optimisticPrefixMatch = false;
                    state.stage = READ_PROB;
                    state.node = root;
                } else if(ops[offset + x] == OP_INSERT) {
                    state.stage       = INSERT_PROB;
                    state.needRestart = false;
                    state.node        = nullptr;
                    state.nextNode    = root;
                    state.parentNode  = nullptr;
                    state.parentKey   = 0; 
                    state.nodeKey     = 0;
                }
                x++;
                prefetch_range(state.key, 64);
                prefetch_range(state.node, 64);
            } else if (state.stage == INSERT_RESTART) {
                state.stage       = INSERT_PROB;
                state.level       = 0;
                state.id          = offset + x;
                state.needRestart = false;
                state.node        = nullptr;
                state.nextNode    = root;
                state.parentNode  = nullptr;
                state.parentKey   = 0; 
                state.nodeKey     = 0;
            } 
            else if (state.stage == READ_PROB) {
                // check if it is leaf node
                // printf("state: %d, level: %d\n", state.id, state.level);
                if (N::isLeaf((N*)state.node)) {
                    // printf("key addr: %llx\n", state.node);
                    // printf("key addr: %llx\n", N::getLeaf((N*)state.node));
                    // return;
                    Key *ret = N::getLeaf((N*)state.node);
                    if (state.level < state.key->getKeyLen() - 1 || state.optimisticPrefixMatch) {
                        // restart_cnt++;
                        return_val(state, num_finished, y, offset, ret_val, checkKey(ret, state.key));
                        continue;
                    } else {
                        return_val(state, num_finished, y, offset, ret_val, &ret->value);
                        continue;
                    }
                }
                NTypes nextNodeType;
                switch (checkPrefix((N*)state.node, state.key, state.level)) { // increases level
                    case CheckPrefixResult::NoMatch: {
                        return_val(state, num_finished, y, offset, ret_val, NULL);
                        continue;                       
                    }
                    case CheckPrefixResult::OptimisticMatch:
                        state.optimisticPrefixMatch = true;
                        // fallthrough
                    case CheckPrefixResult::Match: {
                        if (state.key->getKeyLen() <= state.level) {
                            return_val(state, num_finished, y, offset, ret_val, NULL);
                            continue;
                        }
                        state.node = N::getChild(state.key->fkey[state.level], (N*)state.node);
#ifdef AMAC_DYNAMIC
                        nextNodeType = N::getNodeType((N*)state.node);
                        state.node = N::getNode((N*)state.node);
#endif
                        if (state.node == nullptr) {
                            return_val(state, num_finished, y, offset, ret_val, NULL);
                            continue;
                        }
                    }
                }
                state.level++;
                // prefetch_range(state.node, 64);
                // printf("%d  %d\n", N::isLeaf((N*)state.node), (int)nextNodeType);
                // prefetch_range(state.node, N::node_sizes[(uint8_t)nextNodeType]);
                // prefetch_range_times(state.node, N::node_prefetch[(uint8_t)nextNodeType]);
                prefetch_range_times(state.node, 1);

            } else if (state.stage == INSERT_PROB) {
                //art_cout << "Inserting key " << k->fkey << std::endl;
                // EpocheGuard epocheGuard(epocheInfo);
                restart:

                state.parentNode = state.node;
                state.parentKey = state.nodeKey;
                state.node = state.nextNode;
                auto v = ((N*)state.node)->getVersion();
                uint32_t nextLevel = state.level;
                uint8_t nonMatchingKey;
                Prefix remainingPrefix;
                switch (checkPrefixPessimistic((N*)state.node, state.key, nextLevel, nonMatchingKey, remainingPrefix,
                                                            this->loadKey)) { // increases level
                    case CheckPrefixPessimisticResult::SkippedLevel:
                        goto restart;
                    case CheckPrefixPessimisticResult::NoMatch: {
                        assert(nextLevel < state.key->getKeyLen()); //prevent duplicate key
                        ((N*)state.node)->lockVersionOrRestart(v, state.needRestart);
                        if (state.needRestart) goto restart;
                        // 1) Create new node which will be parent of node, Set common prefix, level to this node
                        Prefix prefi = ((N*)state.node)->getPrefi();
                        prefi.prefixCount = nextLevel - state.level;
                        auto newNode = new N4(nextLevel, prefi);
                        // 2)  add node and (tid, *k) as children
                        newNode->insert(state.key->fkey[nextLevel], N::setLeaf(state.key), false);
                        newNode->insert(nonMatchingKey, (N*)state.node, false);
                        N::clflush((char *)newNode, sizeof(N4), true, true);
                        // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                        ((N*)state.parentNode)->writeLockOrRestart(state.needRestart);
                        if (state.needRestart) {
                            delete newNode;
                            ((N*)state.node)->writeUnlock();
                            goto restart;
                        }
                        N::change((N*)state.parentNode, state.parentKey, newNode);
                        ((N*)state.parentNode)->writeUnlock();
        
                        // 4) update prefix of node, unlock
                        ((N*)state.node)->setPrefix(remainingPrefix.prefix,
                                    ((N*)state.node)->getPrefi().prefixCount - ((nextLevel - state.level) + 1), true);
                        ((N*)state.node)->writeUnlock();
                        return;
            
                    } // end case  NoMatch
                    case CheckPrefixPessimisticResult::Match:
                        break;
                }
                assert(nextLevel < state.key->getKeyLen()); //prevent duplicate key
                state.level = nextLevel;
                state.nodeKey = state.key->fkey[state.level];
                state.nextNode = N::getChild(state.nodeKey, (N*)state.node);
#ifdef AMAC_DYNAMIC
                state.nextNode = N::getNode((N*)state.nextNode);
#endif
                if (state.nextNode == nullptr) {
                    ((N*)state.node)->lockVersionOrRestart(v, state.needRestart);
                    if (state.needRestart) goto restart;
                    N::insertAndUnlock((N*)state.node, (N*)state.parentNode, state.parentKey, 
                        state.nodeKey, N::setLeaf(state.key), threadEpocheInfo, state.needRestart);
                    if (state.needRestart) goto restart;
                        return;
                }
                if (N::isLeaf((N*)state.nextNode)) {
                    ((N*)state.node)->lockVersionOrRestart(v, state.needRestart);
                    if (state.needRestart) goto restart;
                    Key *key;
                    key = N::getLeaf((N*)state.nextNode);
                    state.level++;
                    assert(state.level < key->getKeyLen()); //prevent inserting when prefix of key exists already
                    uint32_t prefixLength = 0;
                    while (key->fkey[state.level + prefixLength] == state.key->fkey[state.level + prefixLength]) {
                        prefixLength++;
                    }
                    auto n4 = new N4(state.id + prefixLength, &state.key->fkey[state.level], prefixLength);
                    n4->insert(state.key->fkey[state.level + prefixLength], N::setLeaf(state.key), false);
                    n4->insert(key->fkey[state.level + prefixLength], (N*)state.nextNode, false);
                    N::clflush((char *)n4, sizeof(N4), true, true);
                    N::change((N*)state.node, state.key->fkey[state.level - 1], n4);
                    ((N*)state.node)->writeUnlock();
                    return;
                }
                state.level++;
            
            }
        }

    }
    
    bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey, Key *result[],
                                std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
        for (uint32_t i = 0; i < std::min(start->getKeyLen(), end->getKeyLen()); ++i) {
            if (start->fkey[i] > end->fkey[i]) {
                resultsFound = 0;
                return false;
            } else if (start->fkey[i] < end->fkey[i]) {
                break;
            }
        }
        EpocheGuard epocheGuard(threadEpocheInfo);
        Key *toContinue = NULL;
        bool restart;
        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                //result[resultsFound] = reinterpret_cast<TID>((N::getLeaf(node))->value);
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != NULL) {
                        break;
                    }
                }
            }
        };
        std::function<void(const N *, uint32_t)> findStart = [&copy, &start, &findStart, &toContinue, &restart, this](
                const N *node, uint32_t level) {
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level, loadKey);
            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start->getKeyLen() > level) ? start->fkey[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, startLevel, 255, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, level + 1);
                        } else if (k > startLevel) {
                            copy(n);
                        }
                        if (toContinue != NULL || restart) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
                case PCCompareResults::Smaller:
                    break;
            }
        };
        std::function<void(const N *, uint32_t)> findEnd = [&copy, &end, &toContinue, &restart, &findEnd, this](
                const N *node, uint32_t level) {
            if (N::isLeaf(node)) {
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level, loadKey);

            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, 0, endLevel, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, level + 1);
                        } else if (k < endLevel) {
                            copy(n);
                        }
                        if (toContinue != NULL || restart) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
            }
        };

        restart:
        restart = false;
        resultsFound = 0;

        uint32_t level = 0;
        N *node = nullptr;
        N *nextNode = root;

        while (true) {
            if (!(node = nextNode) || toContinue) break;
            PCEqualsResults prefixResult;
            prefixResult = checkPrefixEquals(node, level, start, end, loadKey);
            switch (prefixResult) {
                case PCEqualsResults::SkippedLevel:
                    goto restart;
                case PCEqualsResults::NoMatch: {
                    return false;
                }
                case PCEqualsResults::Contained: {
                    copy(node);
                    break;
                }
                case PCEqualsResults::BothMatch: {
                    uint8_t startLevel = (start->getKeyLen() > level) ? start->fkey[level] : 0;
                    uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level] : 255;
                    if (startLevel != endLevel) {
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        N::getChildren(node, startLevel, endLevel, children, childrenCount);
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            const N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, level + 1);
                            } else if (k > startLevel && k < endLevel) {
                                copy(n);
                            } else if (k == endLevel) {
                                findEnd(n, level + 1);
                            }
                            if (restart) {
                                goto restart;
                            }
                            if (toContinue) {
                                break;
                            }
                        }
                    } else {
                        nextNode = N::getChild(startLevel, node);
                        level++;
                        continue;
                    }
                    break;
                }
            }
            break;
        }

        if (toContinue != NULL) {
            continueKey = toContinue;
            return true;
        } else {
            return false;
        }
    }

    void *Tree::checkKey(const Key *ret, const Key *k) const {
        if (ret->getKeyLen() == k->getKeyLen() && memcmp(ret->fkey, k->fkey, k->getKeyLen()) == 0) {
            return &(const_cast<Key *>(ret)->value);
        }
        return NULL;
    }

    void Tree::insert(const Key *k, ThreadInfo &epocheInfo) {
	//art_cout << "Inserting key " << k->fkey << std::endl;
        EpocheGuard epocheGuard(epocheInfo);
        restart:
        // restart_cnt++;
        bool needRestart = false;
	//art_cout << __func__ << "Start/Restarting insert.." << std::endl;
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->getVersion();

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                                           this->loadKey)) { // increases level
                case CheckPrefixPessimisticResult::SkippedLevel:
                    goto restart;
                case CheckPrefixPessimisticResult::NoMatch: {
                    assert(nextLevel < k->getKeyLen()); //prevent duplicate key
                    node->lockVersionOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    Prefix prefi = node->getPrefi();
                    prefi.prefixCount = nextLevel - level;
                    auto newNode = new N4(nextLevel, prefi);

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k->fkey[nextLevel], N::setLeaf(k), false);
                    newNode->insert(nonMatchingKey, node, false);

                    // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                    parentNode->writeLockOrRestart(needRestart);
                    if (needRestart) {
                        delete newNode;
                        node->writeUnlock();
                        goto restart;
                    }
                    N::change(parentNode, parentKey, newNode);
                    parentNode->writeUnlock();
	
                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix.prefix,
                                node->getPrefi().prefixCount - ((nextLevel - level) + 1), true);
                    node->writeUnlock();
                    return;
		
                } // end case  NoMatch
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            assert(nextLevel < k->getKeyLen()); //prevent duplicate key
            level = nextLevel;
            nodeKey = k->fkey[level];
            nextNode = N::getChild(nodeKey, node);
#ifdef AMAC_DYNAMIC
            nextNode = N::getNode(nextNode);
#endif            
            
	        if (nextNode == NULL) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) goto restart;

                N::insertAndUnlock(node, parentNode, parentKey, nodeKey, N::setLeaf(k), epocheInfo, needRestart);
                if (needRestart) goto restart;
                return;
            } 
            if (N::isLeaf(nextNode)) {
                // printf("isleaf\n");
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) goto restart;
		        Key *key;
                key = N::getLeaf(nextNode);

                level++;
                assert(level < key->getKeyLen()); //prevent inserting when prefix of key exists already
                uint32_t prefixLength = 0;
                while (key->fkey[level + prefixLength] == k->fkey[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(level + prefixLength, &k->fkey[level], prefixLength);
                n4->insert(k->fkey[level + prefixLength], N::setLeaf(k), false);
                n4->insert(key->fkey[level + prefixLength], nextNode, false);

                N::change(node, k->fkey[level - 1], n4);
                node->writeUnlock();
                return;
            }
            level++;
        }
    }

    void Tree::remove(const Key *k, ThreadInfo &threadInfo) {
        EpocheGuard epocheGuard(threadInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;
        //bool optimisticPrefixMatch = false;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->getVersion();

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                        goto restart;
                    }
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k->fkey[level];
                    nextNode = N::getChild(nodeKey, node);

                    if (nextNode == nullptr) {
                        if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {//TODO benötigt??
                            goto restart;
                        }
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        node->lockVersionOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        if (!checkKey(N::getLeaf(nextNode), k)) {
                            node->writeUnlock();
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && node != root) {
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                uint64_t vChild = secondNodeN->getVersion();
                                secondNodeN->lockVersionOrRestart(vChild, needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    secondNodeN->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

			     #ifdef CRASH_MERGE
			        pid_t pid = fork();
				if (pid == 0){
					// This is a crash state. So initialize locks
					lock_initialization();
					art_cout << "\n Child process returned before updating level in merge"<< std::endl;	
					return;
				}
				else if (pid > 0) {
					int returnStatus;
					waitpid(pid, &returnStatus, 0);
					art_cout << " Continuing in parent to remove " << k->fkey << std::endl;
			       #endif
                               		secondNodeN->addPrefixBefore(node, secondNodeK);

                                	parentNode->writeUnlock();
                                	node->writeUnlockObsolete();
                                	this->epoche.markNodeForDeletion(node, threadInfo);
                                	secondNodeN->writeUnlock();

				#ifdef CRASH_MERGE
				  }// end parent
				  else {
					art_cout << "Fork failed" << std::endl;
					return;
				  }//end fork fail
				#endif
                            }
                        } else {
                            N::removeAndUnlock(node, k->fkey[level], parentNode, parentKey, threadInfo, needRestart);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                }
            }
        }
    }


    typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key *k, uint32_t &level) {
        if (k->getKeyLen() <= n->getLevel()) {
            return CheckPrefixResult::NoMatch;
        }
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
        if (p.prefixCount > 0) {
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
                 i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
                if (p.prefix[i] != k->fkey[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (p.prefixCount > maxStoredPrefixLength) {
                level += p.prefixCount - maxStoredPrefixLength;
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        // restart_cnt++;

        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key *k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
	//art_cout << __func__ << ":Actual=" << p.prefixCount + level << ",Expected=" << n->getLevel() << std::endl;
        if (p.prefixCount + level != n->getLevel()) {
            // Intermediate or inconsistent state from path compression "split" or "merge" is detected
            // Inconsistent path compressed prefix should be recovered in here
            bool needRecover = false;
            auto v = n->getVersion();
	    art_cout << __func__ << " INCORRECT LEVEL ENCOUNTERED " << std::endl;
            n->lockVersionOrRestart(v, needRecover);
            if (!needRecover) {
                // Inconsistent state due to prior system crash is suspected --> Do recovery
                // TODO: recovery algorithm will be added
                // 1) Picking up arbitrary two leaf nodes and then 2) rebuilding correct compressed prefix
		art_cout << __func__ << " PERFORMING RECOVERY" << std::endl;
                uint32_t discrimination = (n->getLevel() > level ? n->getLevel() - level : level - n->getLevel());
                Key *kr = N::getAnyChildTid(n);
                p.prefixCount = discrimination;
                for (uint32_t i = 0; i < std::min(discrimination, maxStoredPrefixLength); i++)
                    p.prefix[i] = kr->fkey[level + i];
                n->setPrefix(p.prefix, p.prefixCount, true);
                n->writeUnlock();
            }

            // path compression merge is in progress --> restart from root
            // path compression split is in progress --> skipping an intermediate compressed prefix by using level (invariant)
            if (p.prefixCount + level < n->getLevel()) {
                return CheckPrefixPessimisticResult::SkippedLevel;
            }
        }

        if (p.prefixCount > 0) {
            uint32_t prevLevel = level;
            Key *kt = NULL;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    //Optimistic path compression
                    kt = N::getAnyChildTid(n);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
                if (curKey != k->fkey[level]) {
                    nonMatchingKey = curKey;
                    if (p.prefixCount > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            kt = N::getAnyChildTid(n);
                        }
                        for (uint32_t j = 0; j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                                          maxStoredPrefixLength); ++j) {
                            nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
                        }
                    } else {
                        for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                            nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                        }
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key *k, uint32_t &level,
                                                        LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            return PCCompareResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key *kt = NULL;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    //loadKey(N::getAnyChildTid(n), kt);
                    kt = N::getAnyChildTid(n);
                }
                uint8_t kLevel = (k->getKeyLen() > level) ? k->fkey[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key *start, const Key *end,
                                                      LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            return PCEqualsResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key *kt = NULL;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    //loadKey(N::getAnyChildTid(n), kt);
                    kt = N::getAnyChildTid(n);
                }
                uint8_t startLevel = (start->getKeyLen() > level) ? start->fkey[level] : 0;
                uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }
}
