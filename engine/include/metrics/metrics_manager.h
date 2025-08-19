#ifndef VECTOR_DATABASE_METRICS_MANAGER_H
#define VECTOR_DATABASE_METRICS_MANAGER_H

#include <cstdint>
#include <limits>
#include <shared_mutex>
#include <string>

namespace vector_db_engine {

// MetricsManager is responsible for tracking and calculating the per-table metrics for the vector database, including each operation count, search latency, and the cache hits and misses.
class MetricsManager {
public:
    struct Metrics {
        uint64_t insert_count = 0;
        uint64_t remove_count = 0;
        uint64_t search_count = 0;
        uint64_t cleanup_count = 0;
        uint64_t reindex_count = 0;

        float average_search_latency_ms = 0.0f;
        float max_search_latency_ms = 0.0f;
        float min_search_latency_ms = std::numeric_limits<float>::max();

        uint64_t cache_hit = 0;
        uint64_t cache_miss = 0;
    };

    enum class CountType {
        INSERT = 0,
        REMOVE = 1,
        SEARCH = 2,
        CLEANUP = 3,
        REINDEX = 4,
        CACHE_HIT = 5,
        CACHE_MISS = 6
    };

    MetricsManager() = default;

    Metrics GetMetrics() const;

    // Increment the metrics type by the step value input.
    void Increment(CountType count_type, uint64_t step);

    // Calculate the minimum, maximum, and average search latency on each search.
    void CalculateSearchLatency(float latency);

private:
    Metrics metrics_;

    mutable std::shared_mutex metrics_mutex_;
};

} // namespace vector_db_engine


#endif //VECTOR_DATABASE_METRICS_MANAGER_H
