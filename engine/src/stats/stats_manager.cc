#include "stats_manager.h"

#include <mutex>
#include <shared_mutex>

namespace vector_db_engine {

StatsManager::StatsManager()
    : removed_(false)
{}

StatsManager::Stats StatsManager::GetStats() const {
    std::shared_lock lock(stats_mutex_);
    return stats_;
}

bool StatsManager::GetRemovedFlag() const {
    return removed_;
}

void StatsManager::Increment(StatsManager::StatType stat_type) {
    std::unique_lock lock(stats_mutex_);
    switch (stat_type) {
        case StatType::VECTOR:
            stats_.vector_count++;
            break;
        case StatType::DELETED:
            stats_.deleted_count++;
            break;
        case StatType::STALE:
            stats_.stale_count++;
            break;
    }
}

void StatsManager::Reset(StatsManager::StatType stat_type) {
    std::unique_lock lock(stats_mutex_);
    switch (stat_type) {
        case StatType::VECTOR:
            stats_.vector_count = 0;
            break;
        case StatType::DELETED:
            stats_.deleted_count = 0;
            break;
        case StatType::STALE:
            stats_.stale_count = 0;
            break;
    }
}

void StatsManager::SetRemovedFlag(bool value) {
    removed_ = value;
}

} // namespace vector_db_engine
