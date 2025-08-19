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

// ReplicaManager is responsible for replica syncing, both on replica and primary side, depending on which node it exists on. It will either run the replica server on the replicas or the background thread on the primary sending out the sync requests to replicas. This includes triggering the WAL entry application and checksum validation using callbacks.
class ReplicaManager {
public:
    // Creates the replica manager and if on primary, creates the background thread to send out syncs after each interval and if on replica, creates the replica sync server that receives sync requests directly.
    explicit ReplicaManager(const std::string& name, bool primary, const std::string& sync_server_address, const std::vector<std::string>& replicas, int sync_interval, std::atomic<bool>& shutdown, const std::function<bool(const vector_db::WALEntry&)>& apply_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()>& get_persistence_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()>& get_stats_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()>& get_vector_store_map_callback, Logger* logger);

    // Ensures graceful shutdown by joining the background sync thread or stopping the replica sync server.
    ~ReplicaManager();

    // Starts the replica sync server only on the replicas.
    void RunReplicaServer();

    // Starts the sync background thread only on the primary.
    void RunReplicaSyncLoop();

    // Sends a sync RPC to the replicas from the primary. Should only be triggered if data was modified on the primary. If force is true, this operation will run even during shutdown, otherwise it will only run if not shutting down.
    void Sync(bool force);

    // Triggers the entry application on receipt of sync request on replica. This achieves this by using the apply WAL entry callback.
    bool ApplyWALEntry(const vector_db::WALEntry& entry);

    // Gets the vector store checksum directly from the vector store.
    std::uint64_t GetChecksum(std::string table);

private:
    std::string name_;

    // Indicates how often the background thread should check to see if a sync is needed.
    int sync_interval_;

    // Indicates to the background thread/replica sync server to shut down gracefully.
    std::atomic<bool>& shutdown_;

    Logger* logger_;

    std::vector<std::string> replicas_;

    // Callback that applies the WAL entry directly via the engine API.
    std::function<bool(const vector_db::WALEntry&)> apply_callback_;

    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()> get_persistence_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()> get_stats_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()> get_vector_store_map_callback_;

    bool primary_;
    std::string sync_server_address_;

    // Tracks the WAL offset for each replica-table combination to allow for fine-grained updates.
    std::unordered_map<std::string, std::unordered_map<std::string, std::uint64_t>> wal_offsets_per_replica_map_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_H
