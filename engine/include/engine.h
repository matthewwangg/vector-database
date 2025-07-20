#ifndef VECTOR_DATABASE_ENGINE_H
#define VECTOR_DATABASE_ENGINE_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "logger.h"
#include "lru_cache.h"
#include "persistence_manager.h"
#include "thread_pool.h"
#include "vector_store.h"

#include "replica.pb.h"

namespace vector_db_engine {

class Engine {
public:
    struct Metadata {
        std::string name;
        bool primary;
        std::string sync_server_address;
    };

    struct Stats {
        uint64_t vector_count = 0;
        uint64_t deleted_count = 0;
        uint64_t stale_count = 0;
    };

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

    explicit Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string primary_address = "", std::vector<std::string> replicas = {});
    ~Engine();

    bool Insert(std::string table_name, Id id, const Vector& vector, const std::string& content);
    bool Remove(std::string table_name, Id id);
    std::vector<VectorStore::Data> Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param);

    std::vector<bool> BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors);
    std::vector<bool> BatchRemove(std::string table_name, std::vector<Id> ids);
    std::vector<std::vector<VectorStore::Data>> BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests);

    Stats GetStats(std::string table_name);
    Metrics GetMetrics(std::string table_name);

    bool CreateTable(std::string name);
    bool CreateTableWithoutLock(std::string name);
    bool DropTable(std::string name);
    bool DropTableWithoutLock(std::string name);

    void BackgroundCleanupLoop();
    void Cleanup(const std::string& table_name, bool force);

    void BackgroundSyncReplicasLoop();
    void Sync(bool force);

    void BackgroundWaitForSyncLoop();
    void ApplyWALEntry(const vector_db::WALEntry& entry);

    Logger* GetLogger() const;

private:
    Metadata metadata_;
    std::atomic<bool> shutdown_;

    std::vector<std::string> replicas_;
    std::unordered_map<std::string, uint64_t> replica_wal_offsets_map_;

    std::unordered_map<std::string, std::unique_ptr<VectorStore>> store_map_;
    std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>> persistence_manager_map_;
    std::unordered_map<std::string, Stats> stats_map_;
    std::unordered_map<std::string, Metrics> metrics_map_;
    std::unordered_map<std::string, std::atomic<bool>> removed_flag_map_;
    std::unordered_map<std::string, std::unique_ptr<Cache>> cache_map_;

    mutable std::shared_mutex engine_mutex_;

    float reindex_threshold_;
    bool use_cache_;

    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<Logger> logger_;

    std::thread cleanup_thread_;
    std::atomic<bool> removed_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_ENGINE_H
