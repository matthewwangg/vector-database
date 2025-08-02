#include "hnsw_index.h"
#include "local_logger.h"
#include "persistence_manager.h"
#include "vector_store.h"

#include <filesystem>
#include <functional>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class PersistenceManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        logger_ = std::make_unique<LocalLogger>();
        persistence_manager_ = std::make_unique<VectorPersistenceManager>("unit_test", VectorPersistenceManager::StoredIndexType::HNSW, "unit_test_store_snapshot.dat", "unit_test_index_snapshot.dat", "unit_test_wal.log", "unit_test_metadata.bin", logger_.get());

        HNSWIndex::HNSWIndexConfig config = {2, 4, 16, 1.0f, VectorIndex::DistanceMetric::L2, 4};
        auto index = std::make_unique<HNSWIndex>(config);
        store_ = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, std::move(index), 4);
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

TEST_F(PersistenceManagerTest, SaveAndLoadSnapshotWithHNSWIndex) {
    HNSWIndex::HNSWIndexConfig config = {2, 4, 16, 1.0f, VectorIndex::DistanceMetric::L2, 4};
    auto index = std::make_unique<HNSWIndex>(config);
    store_ = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, std::move(index), 4);
    store_->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1");
    store_->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2");

    persistence_manager_->SaveSnapshot(*store_);

    index = std::make_unique<HNSWIndex>(config);
    store_ = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, std::move(index), 4);

    store_ = persistence_manager_->LoadSnapshot();

    auto results = store_->Search({0.5f, 0.4f, 0.6f, 0.2f}, 2, 10);
    EXPECT_THAT(results, ::testing::UnorderedElementsAre(MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1"), MatchData(2, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2")));
}

TEST_F(PersistenceManagerTest, SaveAndLoadSnapshotWithFlatIndex) {
    auto persistence_manager = std::make_unique<VectorPersistenceManager>("unit_test_flat", VectorPersistenceManager::StoredIndexType::FLAT, "unit_test_store_snapshot.dat", "unit_test_index_snapshot.dat", "unit_test_wal.log", "unit_test_metadata.bin", logger_.get());
    FlatIndex::FlatIndexConfig config = {4, VectorIndex::DistanceMetric::L2};
    auto index = std::make_unique<FlatIndex>(config);
    auto store = std::make_unique<VectorStore>(VectorStore::IndexType::FLAT, std::move(index), 4);
    store->Insert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1");
    store->Insert(2, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2");

    persistence_manager->SaveSnapshot(*store);

    index = std::make_unique<FlatIndex>(config);
    store = std::make_unique<VectorStore>(VectorStore::IndexType::FLAT, std::move(index), 4);

    store = persistence_manager->LoadSnapshot();

    persistence_manager->Clear();

    auto results = store->Search({0.5f, 0.4f, 0.6f, 0.2f}, 2, 10);
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

    persistence_manager_->ReplayWAL([this](const vector_db::WALEntry& entry) {
        Vector vector(entry.insert_config().vector().begin(), entry.insert_config().vector().end());
        store_->Insert(entry.insert_config().id(), vector, entry.insert_config().content());
    });

    auto results = store_->Search({0.5f, 0.4f, 0.6f, 0.2f}, 2, 10);
    EXPECT_THAT(results, ::testing::UnorderedElementsAre(MatchData(1, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_1"), MatchData(2, std::vector<float>{0.5f, 0.4f, 0.6f, 0.2f}, "test_content_2")));
}

TEST_F(PersistenceManagerTest, ClearWAL) {
    persistence_manager_->AppendInsert(1, {0.5f, 0.4f, 0.6f, 0.2f}, "test_content");
    persistence_manager_->ClearWAL();
    EXPECT_TRUE(persistence_manager_->SerializeWALEntries(0).empty());
}

} // namespace vector_db_engine
