#include "stats_manager.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class StatsManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        stats_manager_ = std::make_unique<StatsManager>();
    }

    std::unique_ptr<StatsManager> stats_manager_;
};

TEST_F(StatsManagerTest, Increment) {
    stats_manager_->Increment(StatsManager::StatType::VECTOR);
    stats_manager_->Increment(StatsManager::StatType::DELETED);
    stats_manager_->Increment(StatsManager::StatType::STALE);

    EXPECT_EQ(stats_manager_->GetStats().vector_count, 1);
    EXPECT_EQ(stats_manager_->GetStats().deleted_count, 1);
    EXPECT_EQ(stats_manager_->GetStats().stale_count, 1);
}

TEST_F(StatsManagerTest, Reset) {
    stats_manager_->Increment(StatsManager::StatType::VECTOR);
    stats_manager_->Increment(StatsManager::StatType::DELETED);
    stats_manager_->Increment(StatsManager::StatType::STALE);

    ASSERT_EQ(stats_manager_->GetStats().vector_count, 1);
    ASSERT_EQ(stats_manager_->GetStats().deleted_count, 1);
    ASSERT_EQ(stats_manager_->GetStats().stale_count, 1);

    stats_manager_->Reset(StatsManager::StatType::VECTOR);
    stats_manager_->Reset(StatsManager::StatType::DELETED);
    stats_manager_->Reset(StatsManager::StatType::STALE);

    EXPECT_EQ(stats_manager_->GetStats().vector_count, 0);
    EXPECT_EQ(stats_manager_->GetStats().deleted_count, 0);
    EXPECT_EQ(stats_manager_->GetStats().stale_count, 0);
}

TEST_F(StatsManagerTest, Set) {
    stats_manager_->Set(StatsManager::StatType::VECTOR, 10);
    stats_manager_->Set(StatsManager::StatType::DELETED, 20);
    stats_manager_->Set(StatsManager::StatType::STALE, 30);

    EXPECT_EQ(stats_manager_->GetStats().vector_count, 10);
    EXPECT_EQ(stats_manager_->GetStats().deleted_count, 20);
    EXPECT_EQ(stats_manager_->GetStats().stale_count, 30);
}

TEST_F(StatsManagerTest, AdjustForDeletion) {
    stats_manager_->Set(StatsManager::StatType::VECTOR, 20);
    stats_manager_->Set(StatsManager::StatType::DELETED, 10);

    ASSERT_EQ(stats_manager_->GetStats().vector_count, 20);
    ASSERT_EQ(stats_manager_->GetStats().deleted_count, 10);

    stats_manager_->AdjustForDeletions();

    EXPECT_EQ(stats_manager_->GetStats().vector_count, 10);
}

TEST_F(StatsManagerTest, SetRemovedFlag) {
    ASSERT_EQ(stats_manager_->GetRemovedFlag(), false);
    stats_manager_->SetRemovedFlag(true);
    EXPECT_EQ(stats_manager_->GetRemovedFlag(), true);
}

} // namespace vector_db_engine
