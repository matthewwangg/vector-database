#ifndef VECTOR_DATABASE_REPLICA_MANAGER_H
#define VECTOR_DATABASE_REPLICA_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "hnsw_index.h"
#include "logger.h"

#include "replica.pb.h"

namespace vector_db_engine {

class Engine;

class ReplicaManager {
public:
    explicit ReplicaManager(Engine* engine, std::string name, bool primary, std::string sync_server_address, std::vector<std::string> replicas, std::atomic<bool>& shutdown);
    ~ReplicaManager();

    void RunReplicaServer();
    void RunReplicaSyncLoop();

    void Sync(bool force);
    void SyncCreateTable(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size);
    void SyncDropTable(std::string name);

    void ApplyWALEntry(const vector_db::WALEntry& entry);
    bool CreateTableOnReplica(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size);
    bool DropTableOnReplica(std::string name);

private:
    Engine* engine_;

    std::string name_;
    std::atomic<bool>& shutdown_;

    std::vector<std::string> replicas_;

    bool primary_;
    std::string sync_server_address_;
    std::unordered_map<std::string, uint64_t> wal_offsets_map_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_H
