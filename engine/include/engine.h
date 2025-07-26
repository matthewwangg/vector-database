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

#include "cleaner.h"
#include "hnsw_index.h"
#include "logger.h"
#include "lru_cache.h"
#include "metrics_manager.h"
#include "persistence_manager.h"
#include "replica_manager.h"
#include "stats_manager.h"
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

    explicit Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string primary_address = "", std::vector<std::string> replicas = {});
    ~Engine();

    bool Insert(std::string table_name, Id id, const Vector& vector, const std::string& content);
    bool Remove(std::string table_name, Id id);
    std::vector<VectorStore::Data> Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param);

    std::vector<bool> BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors);
    std::vector<bool> BatchRemove(std::string table_name, std::vector<Id> ids);
    std::vector<std::vector<VectorStore::Data>> BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests);

    StatsManager::Stats GetStats(std::string table_name);
    MetricsManager::Metrics GetMetrics(std::string table_name);

    bool CreateTable(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size);
    bool CreateTableOnReplica(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size);
    bool DropTable(std::string name);
    bool DropTableOnReplica(std::string name);
    std::vector<std::string> ListTables();

    void Cleanup(const std::string& table_name, bool force);
    void ApplyWALEntry(const vector_db::WALEntry& entry);

    Logger* GetLogger() const;
    const std::unordered_map<std::string, std::unique_ptr<VectorStore>>& GetStoreMap() const;
    const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>& GetPersistenceManagerMap() const;
    const std::unordered_map<std::string, std::unique_ptr<StatsManager>>& GetStatsManagerMap() const;

private:
    Metadata metadata_;
    std::atomic<bool> shutdown_;

    std::vector<std::string> replicas_;

    std::unordered_map<std::string, std::unique_ptr<VectorStore>> store_map_;
    std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>> persistence_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<StatsManager>> stats_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<MetricsManager>> metrics_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<Cache>> cache_map_;

    mutable std::shared_mutex engine_mutex_;
    std::mutex replica_mutex_;

    float reindex_threshold_;
    bool use_cache_;

    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<Logger> logger_;
    std::unique_ptr<ReplicaManager> replica_manager_;
    std::unique_ptr<Cleaner> cleaner_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_ENGINE_H
