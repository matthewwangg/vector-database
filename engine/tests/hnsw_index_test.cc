#include "hnsw_index.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class HNSWIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        HNSWIndex::HNSWIndexConfig config = {2, 4, 16, 1.0f, VectorIndex::DistanceMetric::L2, 4};
        index_ = std::make_unique<HNSWIndex>(config);
    }

    std::unique_ptr<HNSWIndex> index_;
};

TEST_F(HNSWIndexTest, SuccessInsert) {
    index_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f});

    auto result = index_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 1, 10);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], 1);
}

TEST_F(HNSWIndexTest, SuccessRemove) {
    index_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f});
    index_->Remove(1);

    auto result = index_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 1, 10);
    EXPECT_EQ(result.size(), 0);
}

TEST_F(HNSWIndexTest, SuccessSearchL2) {
    index_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f});
    index_->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f});

    auto result = index_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 2, 10);

    ASSERT_EQ(result.size(), 2);
    EXPECT_THAT(result, ::testing::UnorderedElementsAre(1, 2));
}

TEST_F(HNSWIndexTest, SuccessSearchCosine) {
    HNSWIndex::HNSWIndexConfig config = {2, 4, 16, 1.0f, VectorIndex::DistanceMetric::Cosine, 4};
    index_ = std::make_unique<HNSWIndex>(config);

    index_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f});
    index_->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f});

    auto result = index_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 2, 10);

    ASSERT_EQ(result.size(), 2);
    EXPECT_THAT(result, ::testing::UnorderedElementsAre(1, 2));
}

} // namespace vector_db_engine
