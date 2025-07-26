#include "replica_manager.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "engine.h"
#include "replica_manager_service_impl.h"

namespace vector_db_engine {

constexpr int kSyncInterval = 90;
constexpr int kShutdownCheckInterval = 1000;

ReplicaManager::ReplicaManager(Engine* engine, std::string name, bool primary, std::string sync_server_address, std::vector<std::string> replicas, std::atomic<bool>& shutdown)
    : engine_(engine),
      name_(name),
      primary_(primary),
      sync_server_address_(sync_server_address),
      replicas_(replicas),
      shutdown_(shutdown)
{}

void ReplicaManager::RunReplicaServer() {
    if (sync_server_address_.empty()) {
        engine_->GetLogger()->Warn("no sync server address specified", name_);
        return;
    }

    ReplicaManagerServiceImpl replica_manager_service(engine_);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(sync_server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&replica_manager_service);

    engine_->GetLogger()->Info("replica sync server running on " + sync_server_address_, name_);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::thread shutdown_thread([&server, this]() {
        while (!shutdown_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckInterval));
        }
        engine_->GetLogger()->Info("replica sync server shutting down...", name_);
        server->Shutdown();
    });

    server->Wait();
    shutdown_thread.join();
}

void ReplicaManager::RunReplicaSyncLoop() {
    std::unique_lock<std::mutex> lock(sync_mutex_);
    while (!shutdown_) {
        sync_cv_.wait_for(lock, std::chrono::seconds(kSyncInterval));
        if (shutdown_) {
            break;
        }

        auto start = std::chrono::steady_clock::now();
        engine_->Sync(false);
        auto end = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        engine_->GetLogger()->Info("sync completed in " + std::to_string(duration_ms) + " ms", name_);
    }
}

} // namespace vector_db_engine
