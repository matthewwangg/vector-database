#include "metrics_manager.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace vector_db_engine {

class MetricsManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics_manager_ = std::make_unique<MetricsManager>();
    }

    std::unique_ptr<MetricsManager> metrics_manager_;
};

TEST_F(MetricsManagerTest, Increment) {
    metrics_manager_->Increment(MetricsManager::CountType::INSERT, 10);
    metrics_manager_->Increment(MetricsManager::CountType::REMOVE, 10);
    metrics_manager_->Increment(MetricsManager::CountType::SEARCH, 10);
    metrics_manager_->Increment(MetricsManager::CountType::CLEANUP, 10);
    metrics_manager_->Increment(MetricsManager::CountType::REINDEX, 10);
    metrics_manager_->Increment(MetricsManager::CountType::CACHE_HIT, 10);
    metrics_manager_->Increment(MetricsManager::CountType::CACHE_MISS, 10);

    EXPECT_EQ(metrics_manager_->GetMetrics().insert_count, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().remove_count, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().search_count, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().cleanup_count, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().reindex_count, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().cache_hit, 10);
    EXPECT_EQ(metrics_manager_->GetMetrics().cache_miss, 10);
}

TEST_F(MetricsManagerTest, CalculateSearchLatency) {
    metrics_manager_->CalculateSearchLatency(10);
    metrics_manager_->Increment(MetricsManager::CountType::SEARCH, 1);

    metrics_manager_->CalculateSearchLatency(30);
    metrics_manager_->Increment(MetricsManager::CountType::SEARCH, 1);

    EXPECT_EQ(metrics_manager_->GetMetrics().average_search_latency_ms, 20);
    EXPECT_EQ(metrics_manager_->GetMetrics().max_search_latency_ms, 30);
    EXPECT_EQ(metrics_manager_->GetMetrics().min_search_latency_ms, 10);
}

TEST_F(MetricsManagerTest, CalculateCleanupLatency) {
    metrics_manager_->CalculateCleanupLatency(10);
    metrics_manager_->Increment(MetricsManager::CountType::CLEANUP, 1);

    metrics_manager_->CalculateCleanupLatency(30);
    metrics_manager_->Increment(MetricsManager::CountType::CLEANUP, 1);

    EXPECT_EQ(metrics_manager_->GetMetrics().average_cleanup_latency_ms, 20);
    EXPECT_EQ(metrics_manager_->GetMetrics().max_cleanup_latency_ms, 30);
    EXPECT_EQ(metrics_manager_->GetMetrics().min_cleanup_latency_ms, 10);
}

} // namespace vector_db_engine
