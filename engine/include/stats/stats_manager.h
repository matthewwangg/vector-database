#ifndef VECTOR_DATABASE_STATS_MANAGER_H
#define VECTOR_DATABASE_STATS_MANAGER_H

#include <atomic>
#include <cstdint>
#include <shared_mutex>

namespace vector_db_engine {

// StatsManager is responsible for keeping metadata about the vector database table, including the count of vectors, as well as the count of deleted vectors and stale vectors for the HNSW index cleanup.
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

    // Create stats manager with modified and removed flags defaulting to false.
    StatsManager();

    Stats GetStats() const;
    bool GetModifiedFlag() const;
    bool GetRemovedFlag() const;

    // Update the specified stat type by 1.
    void Increment(StatType stat_type);

    // Set the specified stat type to 0 by using the Set method.
    void Reset(StatType stat_type);

    // Set the specified stat type to the given value.
    void Set(StatType stat_type, int value);

    // Update the vector count based on the number of nodes deleted.
    void AdjustForDeletions();

    // Set the modified flag to the given boolean value.
    void SetModifiedFlag(bool value);

    // Set the removed flag to the given boolean value.
    void SetRemovedFlag(bool value);

private:
    Stats stats_;

    std::atomic<bool> modified_;
    std::atomic<bool> removed_;

    mutable std::shared_mutex stats_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_STATS_MANAGER_H
