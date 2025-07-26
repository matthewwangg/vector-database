#include "hnsw_index.h"
#include "local_logger.h"
#include "persistence_manager.h"
#include "vector_store.h"

#include <filesystem>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class PersistenceManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        logger_ = std::make_unique<LocalLogger>();
        persistence_manager_ = std::make_unique<VectorPersistenceManager>("unit_test", "unit_test_store_snapshot.dat", "unit_test_index_snapshot.dat", "unit_test_wal.log", logger_.get());

        auto index = std::make_unique<HNSWIndex>(2, 4, 16, 1.0f, HNSWIndex::DistanceMetric::L2, 4);
        store_ = std::make_unique<VectorStore>(std::move(index), 4);
    }

    void TearDown() override {
        persistence_manager_->Clear();
    }

    std::unique_ptr<VectorPersistenceManager> persistence_manager_;
    std::unique_ptr<VectorStore> store_;
    std::unique_ptr<LocalLogger> logger_;
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

TEST_F(PersistenceManagerTest, SaveAndLoadSnapshot) {
    auto index = std::make_unique<HNSWIndex>(2, 4, 16, 1.0f, HNSWIndex::DistanceMetric::L2, 4);
    store_ = std::make_unique<VectorStore>(std::move(index), 4);
    store_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1");
    store_->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2");

    persistence_manager_->SaveSnapshot(*store_);

    index = std::make_unique<HNSWIndex>(2, 4, 16, 1.0f, HNSWIndex::DistanceMetric::L2, 4);
    store_ = std::make_unique<VectorStore>(std::move(index), 4);

    store_ = persistence_manager_->LoadSnapshot();

    auto results = store_->Search({0.5f, 0.4f, 0.6f, 0.2f}, 2, 10);
    EXPECT_THAT(results, ::testing::UnorderedElementsAre(MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1"), MatchData(2, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2")));
}

TEST_F(PersistenceManagerTest, AppendInsert) {
    persistence_manager_->AppendInsert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content");

    EXPECT_FALSE(persistence_manager_->SerializeWALEntries(0).empty());
}

TEST_F(PersistenceManagerTest, AppendRemove) {
    persistence_manager_->AppendInsert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content");
    persistence_manager_->AppendRemove(1);

    EXPECT_FALSE(persistence_manager_->SerializeWALEntries(1).empty());
}

TEST_F(PersistenceManagerTest, ReplayWAL) {
    persistence_manager_->AppendInsert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1");
    persistence_manager_->AppendInsert(2, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2");

    persistence_manager_->ReplayWAL(*store_);

    auto results = store_->Search({0.5f, 0.4f, 0.6f, 0.2f}, 2, 10);
    EXPECT_THAT(results, ::testing::UnorderedElementsAre(MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1"), MatchData(2, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2")));
}

TEST_F(PersistenceManagerTest, ClearWAL) {
    persistence_manager_->AppendInsert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content");
    persistence_manager_->ClearWAL();
    EXPECT_TRUE(persistence_manager_->SerializeWALEntries(0).empty());
}

} // namespace vector_db_engine
