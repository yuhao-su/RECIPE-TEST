#include <cstdlib>
#include <atomic>

static inline void lock(bool* lkp) {
    while(!__sync_bool_compare_and_swap(lkp, 0, 1)){
        
    } 
}
static inline void unlock(bool* lkp) {
    *(lkp) = 0;
}

static inline bool try_lock(bool* lkp) {
    return __sync_bool_compare_and_swap(lkp, 0, 1);
}

const uint64_t CACHE_KEY_MASK = 0xffffffUL;

class HashTable
{
private:
    struct HashItem
    {
        std::atomic<uint64_t> t_key;
        void* addr;
        bool lock;
    };
    HashItem *items; 
    uint64_t size;
public:
    void insert(const uint64_t tree_key, void* node_addr) const{
        uint64_t h_key = tree_key % this->size;
        lock(&this->items[h_key].lock);
        this->items[h_key].t_key = tree_key;
        this->items[h_key].addr = node_addr;
        unlock(&this->items[h_key].lock);
    }
    void* find(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        void* ret_addr;
        if (tree_key != items[h_key].t_key.load(std::memory_order_acquire))
            ret_addr = NULL;
        else
            ret_addr = items[h_key].addr;
        if (tree_key != items[h_key].t_key.load(std::memory_order_release))
            ret_addr = NULL; 
        return ret_addr;
    } 
    HashItem* get_ptr(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        return items + h_key;
    } 
    bool check(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        return (tree_key == items[h_key].t_key.load(std::memory_order_relaxed));
    }
    HashTable(int size) {
        this->items = new HashItem[size];
        this->size = size;
    }
    ~HashTable() {
        delete [] items;
    }
};

class HashTableUnsafe
{
private:

    typedef volatile void* HashItem;
    uint64_t size;

public:
    HashItem *items; 
    void insert(const uint64_t tree_key, void* node_addr) const{
        uint64_t h_key = tree_key % this->size;
        this->items[h_key] = node_addr;
    }
    volatile void* find(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        return items[h_key];
    } 
    HashItem* get_ptr(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        return items + h_key;
    } 
    bool check(const uint64_t tree_key) const{
        uint64_t h_key = tree_key % this->size;
        return (items[h_key] != NULL);
    }
    HashTableUnsafe(int size) {
        this->items = (HashItem*)aligned_alloc(64, sizeof(HashItem) * size);
        this->size = size;
        memset(this->items, 0, size * sizeof(HashItem));
    }
    ~HashTableUnsafe() {
        free(items);
    }
};