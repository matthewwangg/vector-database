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

#include "replica.pb.h"

namespace vector_db_engine {

class Engine;

class ReplicaManager {
public:
    explicit ReplicaManager(std::string name, bool primary, std::string sync_server_address, std::vector<std::string> replicas, std::atomic<bool>& shutdown, const std::function<void(const vector_db::WALEntry&)>& apply_callback, std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()> get_persistence_manager_map_callback, Logger* logger);
    ~ReplicaManager();

    void RunReplicaServer();
    void RunReplicaSyncLoop();

    void Sync(bool force);

    void ApplyWALEntry(const vector_db::WALEntry& entry);

private:
    std::string name_;
    std::atomic<bool>& shutdown_;

    Logger* logger_;

    std::vector<std::string> replicas_;

    std::function<void(const vector_db::WALEntry&)> apply_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()> get_persistence_manager_map_callback_;

    bool primary_;
    std::string sync_server_address_;
    std::unordered_map<std::string, uint64_t> wal_offsets_map_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_H
