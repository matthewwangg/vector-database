#include "metrics_manager.h"

#include <cstdint>
#include <mutex>

MetricsManager::Metrics MetricsManager::GetMetrics() const {
    std::shared_lock lock(metrics_mutex_);
    return metrics_;
}

void MetricsManager::Increment(MetricsManager::CountType count_type, uint64_t step) {
    std::unique_lock lock(metrics_mutex_);
    switch (count_type) {
        case CountType::INSERT:
            metrics_.insert_count += step;
            break;
        case CountType::REMOVE:
            metrics_.remove_count += step;
            break;
        case CountType::SEARCH:
            metrics_.search_count += step;
            break;
        case CountType::CLEANUP:
            metrics_.cleanup_count += step;
            break;
        case CountType::REINDEX:
            metrics_.reindex_count += step;
            break;
        case CountType::CACHE_HIT:
            metrics_.cache_hit += step;
            break;
        case CountType::CACHE_MISS:
            metrics_.cache_miss += step;
            break;
    }
}

void MetricsManager::CalculateSearchLatency(float latency) {
    std::unique_lock lock(metrics_mutex_);
    metrics_.average_search_latency_ms = ((metrics_.average_search_latency_ms * metrics_.search_count) + latency) / (metrics_.search_count + 1);
    metrics_.max_search_latency_ms = std::max(metrics_.max_search_latency_ms, latency);
    metrics_.min_search_latency_ms = std::min(metrics_.min_search_latency_ms, latency);
}
