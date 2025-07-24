#ifndef VECTOR_DATABASE_METRICS_MANAGER_H
#define VECTOR_DATABASE_METRICS_MANAGER_H

#include <cstdint>
#include <limits>
#include <shared_mutex>
#include <string>

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

    void Increment(CountType count_type, uint64_t step);
    void CalculateSearchLatency(float latency);

private:
    Metrics metrics_;

    mutable std::shared_mutex metrics_mutex_;
};

#endif //VECTOR_DATABASE_METRICS_MANAGER_H
