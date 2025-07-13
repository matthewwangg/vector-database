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

    virtual std::optional<CacheEntry> Get(const CacheKey& key) = 0;
    virtual void Invalidate(const CacheKey& key) = 0;
    virtual void Store(const CacheKey& key, const CacheEntry& entry) = 0;

    virtual ~Cache() = default;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_CACHE_H
