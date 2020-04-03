#include <Key.h>

namespace ART_ROWEX {
    class Group {
        class State {
            Key key;
            void* node;
            uint32_t level;
            uint64_t value;
        };
    private:
        State* states;
        uint32_t group_size;
    Group(uint32_t group_size) {
        this->group_size = group_size;
        states = (State*)aligned_alloc(64, sizeof(State) * group_size);
    }
    ~Group() {
        free(states);
    }
    };
}