#include "lru_cache.h"

#include <mutex>
#include <optional>

namespace vector_db_engine {

LRUCache::LRUCache(std::size_t max_cache_size)
    : max_cache_size_(max_cache_size)
{}

std::optional<LRUCache::CacheEntry> LRUCache::Get(const CacheKey& key) {
    std::unique_lock write_lock(cache_mutex_);
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return std::nullopt;
    }

    keys_.erase(it->second.second);
    keys_.push_back(key);
    it->second.second = std::prev(keys_.end());

    return it->second.first;
}

void LRUCache::Invalidate(const CacheKey& key) {
    std::unique_lock lock(cache_mutex_);
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return;
    }

    keys_.erase(it->second.second);
    cache_.erase(key);
}

void LRUCache::InvalidateAll() {
    std::unique_lock lock(cache_mutex_);

    cache_.clear();
    keys_.clear();
}

void LRUCache::Store(const CacheKey& key, const CacheEntry& entry) {
    std::unique_lock lock(cache_mutex_);
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
