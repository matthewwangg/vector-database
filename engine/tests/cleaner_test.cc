#include "cleaner.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "local_logger.h"
#include "stats_manager.h"
#include "vector_store.h"

namespace vector_db_engine {

class CleanerTest : public ::testing::Test {
protected:
    void SetUp() override {
        shutdown_ = false;
        logger_ = std::make_unique<LocalLogger>();

        cleanup_callback_ = [this](std::string table, bool force) {
            callback_triggered_ = true;
            callback_table_ = table;
        };

        get_stats_manager_map_callback_ = [this]() -> const auto& {
            return stats_manager_map_;
        };

        get_store_map_callback_ = [this]() -> const auto& {
            return store_map_;
        };

        callback_triggered_ = false;
        callback_table_ = "";
    }

    std::unique_ptr<Logger> logger_;
    std::atomic<bool> shutdown_;

    std::unordered_map<std::string, std::unique_ptr<StatsManager>> stats_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<VectorStore>> store_map_;

    std::function<void(const std::string&, bool)> cleanup_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()> get_stats_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()> get_store_map_callback_;

    bool callback_triggered_;
    std::string callback_table_;
};

TEST_F(CleanerTest, CallbackTriggers) {
    auto store = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, nullptr, 384);
    auto stats_manager = std::make_unique<StatsManager>();
    stats_manager->SetRemovedFlag(true);

    store_map_["test_table"] = std::move(store);
    stats_manager_map_["test_table"] = std::move(stats_manager);

    {
        auto cleaner = std::make_unique<Cleaner>("unit-test", shutdown_, cleanup_callback_, get_store_map_callback_, get_stats_manager_map_callback_, logger_.get());
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        shutdown_ = true;
    }

    EXPECT_TRUE(callback_triggered_);
    EXPECT_EQ(callback_table_, "test_table");
}

TEST_F(CleanerTest, CallbackDoesNotTrigger) {
    auto store = std::make_unique<VectorStore>(VectorStore::IndexType::HNSW, nullptr, 384);
    auto stats_manager = std::make_unique<StatsManager>();
    stats_manager->SetRemovedFlag(false);

    store_map_["test_table"] = std::move(store);
    stats_manager_map_["test_table"] = std::move(stats_manager);

    {
        auto cleaner = std::make_unique<Cleaner>("unit-test", shutdown_, cleanup_callback_, get_store_map_callback_, get_stats_manager_map_callback_, logger_.get());
        shutdown_ = true;
    }

    EXPECT_FALSE(callback_triggered_);
    EXPECT_EQ(callback_table_, "");
}

} // namespace vector_db_engine