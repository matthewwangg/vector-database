#include "lru_cache.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class LRUCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        lru_cache_ = std::make_unique<LRUCache>(2);
    }
    std::unique_ptr<LRUCache> lru_cache_;
};

TEST_F(LRUCacheTest, Get) {
    EXPECT_FALSE(lru_cache_->Get(1).has_value());
}

TEST_F(LRUCacheTest, Store) {
    lru_cache_->Store(1, LRUCache::CacheEntry{});

    EXPECT_TRUE(lru_cache_->Get(1).has_value());
}

TEST_F(LRUCacheTest, Invalidate) {
    lru_cache_->Store(1, LRUCache::CacheEntry{});
    lru_cache_->Invalidate(1);

    EXPECT_FALSE(lru_cache_->Get(1).has_value());
}

TEST_F(LRUCacheTest, InvalidateAll) {
    lru_cache_->Store(1, LRUCache::CacheEntry{});
    lru_cache_->Store(2, LRUCache::CacheEntry{});
    lru_cache_->InvalidateAll();

    EXPECT_FALSE(lru_cache_->Get(1).has_value());
    EXPECT_FALSE(lru_cache_->Get(2).has_value());
}

} // namespace vector_db_engine
