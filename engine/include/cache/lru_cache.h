#ifndef VECTOR_DATABASE_LRU_CACHE_H
#define VECTOR_DATABASE_LRU_CACHE_H

#include <cstddef>
#include <list>
#include <shared_mutex>
#include <optional>
#include <unordered_map>

#include "cache.h"

namespace vector_db_engine {

// LRUCache is the cache that stores key-value pairs and evicts the least recently used pair when needed.
class LRUCache : public Cache {
public:
    // Creates the cache and initializes the cache capacity.
    explicit LRUCache(std::size_t max_cache_size);

    // Retrieve the value in the LRU cache at the cache key. Returns the value on success and null on failure.
    std::optional<CacheEntry> Get(const CacheKey& key) override;

    // Invalidate the value in the LRU cache at the given key.
    void Invalidate(const CacheKey& key) override;

    // Clear the LRU cache completely to indicate that the cache is stale, typically on writes with HNSW index.
    void InvalidateAll() override;

    // Store a key-value pair in the LRU cache directly.
    void Store(const CacheKey& key, const CacheEntry& entry) override;

    std::size_t GetMaxCacheSize() const { return max_cache_size_; }

private:
    std::unordered_map<CacheKey, std::pair<CacheEntry, std::list<CacheKey>::iterator>> cache_;
    std::list<CacheKey> keys_;

    std::size_t max_cache_size_;

    mutable std::shared_mutex cache_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_LRU_CACHE_H
