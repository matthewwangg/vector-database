#ifndef VECTOR_DATABASE_REPLICA_MANAGER_H
#define VECTOR_DATABASE_REPLICA_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "hnsw_index.h"
#include "logger.h"
#include "persistence_manager.h"
#include "stats_manager.h"
#include "vector_store.h"

#include "replica.pb.h"

namespace vector_db_engine {

class ReplicaManager {
public:
    explicit ReplicaManager(const std::string& name, bool primary, const std::string& sync_server_address, const std::vector<std::string>& replicas, int sync_interval, std::atomic<bool>& shutdown, const std::function<bool(const vector_db::WALEntry&)>& apply_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()>& get_persistence_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()>& get_stats_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()>& get_vector_store_map_callback, Logger* logger);
    ~ReplicaManager();

    void RunReplicaServer();
    void RunReplicaSyncLoop();

    void Sync(bool force);

    bool ApplyWALEntry(const vector_db::WALEntry& entry);

    std::uint64_t GetChecksum(std::string table);

private:
    std::string name_;
    int sync_interval_;
    std::atomic<bool>& shutdown_;

    Logger* logger_;

    std::vector<std::string> replicas_;

    std::function<bool(const vector_db::WALEntry&)> apply_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()> get_persistence_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()> get_stats_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()> get_vector_store_map_callback_;

    bool primary_;
    std::string sync_server_address_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::uint64_t>> wal_offsets_per_replica_map_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_H
