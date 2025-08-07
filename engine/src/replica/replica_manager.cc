#include "replica_manager.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "persistence_manager.h"

#include "replica_manager_service_impl.h"
#include "replica.pb.h"

namespace vector_db_engine {

constexpr int kSyncInterval = 90;
constexpr int kShutdownCheckInterval = 1000;
constexpr int kRetryCount = 3;

ReplicaManager::ReplicaManager(std::string name, bool primary, std::string sync_server_address, std::vector<std::string> replicas, std::atomic<bool>& shutdown, std::atomic<bool>& modified, const std::function<void(const vector_db::WALEntry&)>& apply_callback, std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()> get_persistence_manager_map_callback, Logger* logger)
    : name_(name),
      primary_(primary),
      sync_server_address_(sync_server_address),
      replicas_(replicas),
      shutdown_(shutdown),
      modified_(modified),
      apply_callback_(apply_callback),
      get_persistence_manager_map_callback_(get_persistence_manager_map_callback),
      logger_(logger)
{
    if (primary_ && !replicas_.empty()) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaSyncLoop, this);
    } else if (!primary_) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaServer, this);
    }
}

ReplicaManager::~ReplicaManager() {
    {
        std::unique_lock<std::mutex> lock(sync_mutex_);
    }
    sync_cv_.notify_one();
    if (sync_thread_.joinable()) {
        sync_thread_.join();
    }
}

void ReplicaManager::RunReplicaServer() {
    if (sync_server_address_.empty()) {
        logger_->Warn("no sync server address specified", name_);
        return;
    }

    ReplicaManagerServiceImpl replica_manager_service(this);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(sync_server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&replica_manager_service);

    logger_->Info("replica sync server running on " + sync_server_address_, name_);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::thread shutdown_thread([&server, this]() {
        std::unique_lock<std::mutex> lock(sync_mutex_);
        sync_cv_.wait(lock, [this]() {
            return shutdown_.load();
        });
        logger_->Info("replica sync server shutting down...", name_);
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

        if (!modified_) {
            continue;
        }

        auto start = std::chrono::steady_clock::now();
        Sync(false);
        auto end = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        logger_->Info("sync completed in " + std::to_string(duration_ms) + " ms", name_);
    }
}

void ReplicaManager::Sync(bool force) {
    if ((shutdown_ && !force)) {
        return;
    }

    const auto& persistence_manager_map = get_persistence_manager_map_callback_();
    for (const auto& [table, persistence_manager] : persistence_manager_map) {
        vector_db::SyncRequest request;
        std::vector<vector_db::WALEntry> entries = persistence_manager->SerializeWALEntries(wal_offsets_map_[table]);
        if (entries.empty()) {
            continue;
        }

        for (const auto& entry : entries) {
            *request.add_entry() = entry;
        }

        for (const auto& replica : replicas_) {
            for (int i = 0; i < kRetryCount; ++i) {
                auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
                vector_db::SyncResponse response;
                grpc::ClientContext context;
                grpc::Status status = stub->Sync(&context, request, &response);
                if (status.ok()) {
                    break;
                } else {
                    logger_->Error("error in updating replica: " + replica, name_);
                }
            }
        }

        wal_offsets_map_[table] += entries.size();
    }
    modified_ = false;
}

void ReplicaManager::ApplyWALEntry(const vector_db::WALEntry& entry) {
    apply_callback_(entry);
}

} // namespace vector_db_engine
