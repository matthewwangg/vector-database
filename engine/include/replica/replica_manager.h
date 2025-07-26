#ifndef VECTOR_DATABASE_REPLICA_MANAGER_H
#define VECTOR_DATABASE_REPLICA_MANAGER_H

#include "logger.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace vector_db_engine {

class Engine;

class ReplicaManager {
public:
    explicit ReplicaManager(Engine* engine, std::string name, bool primary, std::string sync_server_address, std::vector<std::string> replicas, std::atomic<bool>& shutdown);
    ~ReplicaManager();

    void RunReplicaServer();
    void RunReplicaSyncLoop();

private:
    Engine* engine_;

    std::string name_;
    std::atomic<bool>& shutdown_;

    bool primary_;
    std::string sync_server_address_;
    std::vector<std::string> replicas_;

    std::thread sync_thread_;
    std::condition_variable sync_cv_;
    std::mutex sync_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_REPLICA_MANAGER_H
