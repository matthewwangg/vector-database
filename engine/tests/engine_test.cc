#include "engine.h"
#include "hnsw_index.h"
#include "local_logger.h"
#include "persistence_manager.h"
#include "vector_store.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class EngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string name = "unit_test";
        std::string server_address = "0.0.0.0:50051";
        bool primary = true;
        float reindex_threshold = 0.25f;
        bool use_cache = false;
        std::string sync_server_address;
        std::vector<std::string> replicas;

        engine_ = std::make_unique<vector_db_engine::Engine>(name, primary, reindex_threshold, use_cache, sync_server_address, replicas);
        engine_->DropTable("test_table");
    }

    void TearDown() override {
        engine_->DropTable("test_table");
    }

    std::vector<float> MakeVector() {
        std::vector<float> vector;
        for (int i = 0; i < 384; ++i) {
            vector.push_back(static_cast<float>(rand()) / RAND_MAX);
        }
        return vector;
    }

    std::unique_ptr<vector_db_engine::Engine> engine_;
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

TEST_F(EngineTest, CreateTable) {
    std::string table = "test_table";
    EXPECT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));
}

TEST_F(EngineTest, DropTable) {
    std::string table = "test_table";
    ASSERT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));

    EXPECT_TRUE(engine_->DropTable(table));
}

TEST_F(EngineTest, ListTables) {
    std::string table = "test_table";
    ASSERT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));

    auto tables = engine_->ListTables();
    ASSERT_EQ(tables.size(), 1);
    EXPECT_EQ(tables[0], table);
}

TEST_F(EngineTest, Insert) {
    std::string table = "test_table";
    ASSERT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));

    EXPECT_TRUE(engine_->Insert(table, 1, MakeVector(), "test_1"));
}

TEST_F(EngineTest, Remove) {
    std::string table = "test_table";
    ASSERT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));

    std::vector<float> vector = MakeVector();

    ASSERT_TRUE(engine_->Insert(table, 1, vector, "test_1"));
    EXPECT_TRUE(engine_->Remove(table, 1));
}

TEST_F(EngineTest, Search) {
    std::string table = "test_table";
    ASSERT_TRUE(engine_->CreateTable(table, 384, 16, 32, 64, 1.0f, vector_db_engine::HNSWIndex::DistanceMetric::L2, 32));

    std::vector<float> vector = MakeVector();

    ASSERT_TRUE(engine_->Insert(table, 1, vector, "test_1"));

    auto result = engine_->Search(table, vector, 1, 10);
    ASSERT_EQ(result.size(), 1);
    EXPECT_THAT(result[0], MatchData(1, vector, "test_1"));
}


} // namespace vector_db_engine
