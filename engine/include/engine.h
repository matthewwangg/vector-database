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
#include "flat_index.h"
#include "hnsw_index.h"
#include "input_validator.h"
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

// Engine is the central API for all vector database operations. It coordinates all core operations, table management, data persistence, replication, cleanup. It also owns all pointers related to the database abstractions except index, which is owned by vector store. Operations are thread-safe at a table level.
class Engine {
public:
    struct Metadata {
        std::string name;
        bool primary;
        std::string sync_server_address;
        std::vector<std::string> replicas;
        float reindex_threshold;
        bool use_cache;
        int cleanup_interval;
        int sync_interval;

        enum class LoggerType {
            SILENT,
            LOCAL,
            REMOTE,
        };

        LoggerType logger_type;
    };

    // Create the engine cleanly by initializing all metadata, setting up all background threads, and loading any previously saved snapshot from snapshot files.
    explicit Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string primary_address = "", std::vector<std::string> replicas = {}, int cleanup_interval = 60, int sync_interval = 90, Metadata::LoggerType logger_type = Metadata::LoggerType::SILENT);

    // Ensures graceful shutdown by cleaning up the index, syncing with all replicas, saving the snapshot and metadata, and clearing the WAL.
    ~Engine();

    // Inserts a single vector into a given table. Returns true on success and false on failure.
    bool Insert(std::string table_name, Id id, const Vector& vector, const std::string& content);

    // Removes a single vector from a given table. Returns true on success and false on failure. Note that this is a soft remove on HNSW index, and will be fully cleaned up by the cleaner at a time interval.
    bool Remove(std::string table_name, Id id);

    // Searches the table for the k-nearest neighbors (or approximate nearest neighbors) to the query vector. Returns the neighbors on success and an empty vector on failure.
    std::vector<VectorStore::Data> Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param);

    // Inserts a batch of vectors into a given table. Returns a vector of booleans to indicate success and failure of each operation.
    std::vector<bool> BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors);

    // Removes a batch of vectors from a given table. Returns a vector of booleans to indicate success and failure of each operation. Note that this is a soft remove on HNSW index, and will be fully cleaned up by the cleaner at a time interval.
    std::vector<bool> BatchRemove(std::string table_name, std::vector<Id> ids);

    // Searches the table for the k-nearest neighbors (or approximate nearest neighbors) to each of the query vectors. Returns a vector of the neighbors on success and an empty vector on failure.
    std::vector<std::vector<VectorStore::Data>> BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests);

    // Gets the database statistics. Returns the stats on success and an empty struct on failure.
    StatsManager::Stats GetStats(std::string table_name);

    // Gets the database metrics. Returns the metrics on success and an empty struct on failure.
    MetricsManager::Metrics GetMetrics(std::string table_name);

    // Create a database table with the given parameters. Returns true on success and false on failure. This should only run on the primary.
    bool CreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);

    // Create a database table with the given parameters. Returns true on success and false on failure. This should only run on the replica and so needs an additional mutex.
    bool CreateTableOnReplica(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size);

    // Drops the database table with the given name. Returns true on success and false on failure. This should only run on the primary.
    bool DropTable(std::string name);

    // Drops the database table with the given name. Returns true on success and false on failure. This should only run on the replica and so needs an additional mutex.
    bool DropTableOnReplica(std::string name);

    // Lists the table names that currently exist. Returns a vector of the table names on success and an empty vector on false.
    std::vector<std::string> ListTables();

    // Cleans up the soft-removed nodes from the HNSW index. This is not needed for other types of indexes.
    void Cleanup(const std::string& table_name, bool force);

    // Apply the WAL entry received from the primary. Should only run on the replicas.
    bool ApplyWALEntry(const vector_db::WALEntry& entry);

    Logger* GetLogger() const;

private:
    Metadata metadata_;

    // Indicates to all threads to shut down gracefully.
    std::atomic<bool> shutdown_;

    // Holds table-specific objects in maps keyed by table name.
    std::unordered_map<std::string, std::unique_ptr<VectorStore>> store_map_;
    std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>> persistence_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<StatsManager>> stats_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<MetricsManager>> metrics_manager_map_;
    std::unordered_map<std::string, std::unique_ptr<Cache>> cache_map_;

    // Protects concurrent access to per-table maps and state.
    mutable std::shared_mutex engine_mutex_;

    // Serializes operations on replicas. Never comes into play on primary, as it is only held on cleanup and replicas applying WAL entries.
    std::mutex replica_mutex_;

    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<InputValidator> input_validator_;
    std::unique_ptr<Logger> logger_;
    std::unique_ptr<ReplicaManager> replica_manager_;
    std::unique_ptr<Cleaner> cleaner_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_ENGINE_H
