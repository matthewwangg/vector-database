#ifndef VECTOR_DATABASE_LRU_CACHE_H
#define VECTOR_DATABASE_LRU_CACHE_H

#include <cstddef>
#include <list>
#include <shared_mutex>
#include <optional>
#include <unordered_map>

#include "cache.h"

namespace vector_db_engine {

class LRUCache : public Cache {
public:
    explicit LRUCache(std::size_t max_cache_size);

    std::optional<CacheEntry> Get(const CacheKey& key) override;
    void Invalidate(const CacheKey& key) override;
    void Store(const CacheKey& key, const CacheEntry& entry) override;

private:
    std::unordered_map<CacheKey, std::pair<CacheEntry, std::list<CacheKey>::iterator>> cache_;
    std::list<CacheKey> keys_;

    std::size_t max_cache_size_;

    mutable std::shared_mutex cache_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_LRU_CACHE_H
