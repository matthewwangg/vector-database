#ifndef VECTOR_DATABASE_CACHE_H
#define VECTOR_DATABASE_CACHE_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace vector_db_engine {

using CacheKey = std::size_t;
using Id = std::uint64_t;
using Vector = std::vector<float>;

// Cache is the interface for all the types of caches.
class Cache {
public:
    struct CacheEntry {
        struct Data {
            Id id;
            Vector vector;
            std::string content;
        };
        std::vector<std::vector<Data>> data;
    };

    virtual ~Cache() = default;

    // Retrieve the value in the cache at the cache key. Returns the value on success and null on failure.
    virtual std::optional<CacheEntry> Get(const CacheKey& key) = 0;

    // Invalidate the value in the cache at the given key.
    virtual void Invalidate(const CacheKey& key) = 0;

    // Clear the cache completely to indicate that the cache is stale, typically on writes with HNSW index.
    virtual void InvalidateAll() = 0;

    // Store a key-value pair in the cache directly.
    virtual void Store(const CacheKey& key, const CacheEntry& entry) = 0;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_CACHE_H
