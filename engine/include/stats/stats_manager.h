#ifndef VECTOR_DATABASE_STATS_MANAGER_H
#define VECTOR_DATABASE_STATS_MANAGER_H

#include <atomic>
#include <cstdint>
#include <shared_mutex>

namespace vector_db_engine {

class StatsManager {
public:
    struct Stats {
        uint64_t vector_count = 0;
        uint64_t deleted_count = 0;
        uint64_t stale_count = 0;
    };

    enum class StatType {
        VECTOR = 0,
        DELETED = 1,
        STALE = 2,
    };

    StatsManager();

    Stats GetStats() const;
    bool GetRemovedFlag() const;

    void Increment(StatType stat_type);
    void Reset(StatType stat_type);
    void Set(StatType stat_type, int value);

    void SetRemovedFlag(bool value);

private:
    Stats stats_;
    std::atomic<bool> removed_;

    mutable std::shared_mutex stats_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_STATS_MANAGER_H
