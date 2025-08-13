#include "stats_manager.h"

#include <mutex>
#include <shared_mutex>

namespace vector_db_engine {

StatsManager::StatsManager()
    : modified_(false),
      removed_(false)
{}

StatsManager::Stats StatsManager::GetStats() const {
    std::shared_lock lock(stats_mutex_);
    return stats_;
}

bool StatsManager::GetModifiedFlag() const {
    return modified_;
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

void StatsManager::Set(StatsManager::StatType stat_type, int value) {
    std::unique_lock lock(stats_mutex_);
    switch (stat_type) {
        case StatType::VECTOR:
            stats_.vector_count = value;
            break;
        case StatType::DELETED:
            stats_.deleted_count = value;
            break;
        case StatType::STALE:
            stats_.stale_count = value;
            break;
    }
}

void StatsManager::AdjustForDeletions() {
    std::unique_lock lock(stats_mutex_);
    stats_.vector_count = stats_.vector_count - stats_.deleted_count;
}

void StatsManager::SetRemovedFlag(bool value) {
    removed_ = value;
}

void StatsManager::SetModifiedFlag(bool value) {
    modified_ = value;
}

} // namespace vector_db_engine
