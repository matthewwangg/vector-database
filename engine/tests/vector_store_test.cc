#include "hnsw_index.h"
#include "vector_store.h"

#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class VectorStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        HNSWIndex::HNSWIndexConfig config = {2, 4, 16, 1.0f, VectorIndex::DistanceMetric::L2, 4};
        auto index = std::make_unique<HNSWIndex>(config);
        store_ = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, std::move(index), 4);
    }

    std::unique_ptr<VectorStore> store_;
};

MATCHER_P3(MatchData, expected_id, expected_vector, expected_content, "") {
    if (arg.id != expected_id) {
        return false;
    }
    if (arg.vector != expected_vector) {
        return false;
    }
    if (arg.content != expected_content) {
        return false;
    }
    return true;
}

TEST_F(VectorStoreTest, SuccessInsert) {
    store_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_1");

    auto result = store_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 1, 10);
    ASSERT_EQ(result.size(), 1);
    EXPECT_THAT(result[0], MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_1"));
}

TEST_F(VectorStoreTest, SuccessRemove) {
    store_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_1");
    store_->Remove(1);

    auto result = store_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 1, 10);
    EXPECT_EQ(result.size(), 0);
}

TEST_F(VectorStoreTest, SuccessSearch) {
    store_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_1");
    store_->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f}, "test_2");

    auto result = store_->Search({0.4f, 0.3f, 0.5f, 0.1f}, 2, 10);

    ASSERT_EQ(result.size(), 2);
    EXPECT_THAT(result, ::testing::UnorderedElementsAre(MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_1"), MatchData(2, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_2")));
}


} // namespace vector_db_engine