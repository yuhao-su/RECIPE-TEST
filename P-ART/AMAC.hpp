#include "../Key.h"
#include "Epoche.h"
#include <xmmintrin.h>
#define CACHELINE_SIZE 64
namespace ART_ROWEX {
    enum {
        OP_INSERT,
        OP_READ,
        OP_SCAN,
        OP_DELETE,
    };
    inline void prefetch_range(void *addr, size_t len) {
        uint8_t *cp;
        uint8_t *end = (uint8_t*)addr + len;
        for (cp = (uint8_t*)addr; cp < end; cp += CACHELINE_SIZE)
            _mm_prefetch(cp, _MM_HINT_NTA);
    } 
    inline void prefetch_range_times(void *addr, size_t len) {
        uint8_t *cp = (uint8_t*)addr;
        for (volatile int i = 0; i < len; i++) {
            _mm_prefetch(cp, _MM_HINT_NTA);
            cp += CACHELINE_SIZE;
        }
    } 
    enum Stages {INIT, READ_INIT, WRITE_INIT, READ_PROB, INSERT_PROB, INSERT_RESTART};
    struct State {
        Key* key;
        void* node;
        void* parentNode;
        void* nextNode;
        uint8_t parentKey, nodeKey;
        uint32_t level;
        uint64_t value;
        uint64_t id;
        bool needRestart;
        bool optimisticPrefixMatch;
        Stages stage;
    };
    class Group {
        
    private:
        State* states;
        uint32_t group_size;
        uint32_t p;
    public:
        State& next_state() {
            p %= group_size;
            return states[p++];
        }
        Group(uint32_t group_size) {
            this->group_size = group_size;
            p = 0;
            states = (State*)aligned_alloc(64, sizeof(State) * group_size);
            for(int i = 0; i < group_size; i++) {
                states[i].stage = INIT;
            }
        }
        ~Group() {
            free(states);
        }
    }; 
}