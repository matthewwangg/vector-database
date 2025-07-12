#include "lru_cache.h"

#include <optional>

namespace vector_db_engine {

LRUCache::LRUCache(std::size_t max_cache_size)
    : max_cache_size_(max_cache_size)
{}

std::optional<LRUCache::CacheEntry> LRUCache::Get(const CacheKey& key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return std::nullopt;
    }

    keys_.erase(it->second.second);
    keys_.push_back(key);
    it->second.second = std::prev(keys_.end());

    return cache_.at(key).first;
}

void LRUCache::Invalidate(const CacheKey& key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return;
    }

    keys_.erase(it->second.second);
    cache_.erase(key);
}

void LRUCache::Store(const CacheKey& key, const CacheEntry& entry) {
    if (auto it = cache_.find(key); it != cache_.end()) {
        keys_.erase(it->second.second);
        keys_.push_back(key);
        it->second = {entry, std::prev(keys_.end())};
        return;
    }

    if (cache_.size() == max_cache_size_) {
        cache_.erase(keys_.front());
        keys_.pop_front();
    }

    keys_.push_back(key);
    cache_[key] = {entry, std::prev(keys_.end())};
}

}
