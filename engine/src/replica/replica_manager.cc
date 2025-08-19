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
#include "stats_manager.h"
#include "vector_store.h"

#include "replica_manager_service_impl.h"
#include "replica.pb.h"

namespace vector_db_engine {

constexpr int kShutdownCheckInterval = 1000;
constexpr int kRetryCount = 3;

ReplicaManager::ReplicaManager(const std::string& name, bool primary, const std::string& sync_server_address, const std::vector<std::string>& replicas, int sync_interval, std::atomic<bool>& shutdown, const std::function<bool(const vector_db::WALEntry&)>& apply_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>>&()>& get_persistence_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()>& get_stats_manager_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()>& get_vector_store_map_callback, Logger* logger)
    : name_(name),
      primary_(primary),
      sync_server_address_(sync_server_address),
      replicas_(replicas),
      sync_interval_(sync_interval),
      shutdown_(shutdown),
      apply_callback_(apply_callback),
      get_persistence_manager_map_callback_(get_persistence_manager_map_callback),
      get_stats_manager_map_callback_(get_stats_manager_map_callback),
      get_vector_store_map_callback_(get_vector_store_map_callback),
      logger_(logger)
{
    if (primary_ && !replicas_.empty()) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaSyncLoop, this);
    } else if (!primary_) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaServer, this);
    } else {
        // If no replicas specified on the primary, do nothing.
    }
}

ReplicaManager::~ReplicaManager() {
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

    // Create a background thread to wait for the shutdown trigger.
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
        sync_cv_.wait_for(lock, std::chrono::milliseconds(sync_interval_));
        if (shutdown_) {
            break;
        }

        bool modified = false;
        const auto& stats_manager_map = get_stats_manager_map_callback_();
        for (const auto& [table, stats_manager] : stats_manager_map) {
            if (stats_manager->GetModifiedFlag() == true) {
                modified = true;
                break;
            }
        }
        if (!modified) {
            continue;
        }

        // Measure the latency of the sync operation.
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
    const auto& vector_store_map = get_vector_store_map_callback_();
    const auto& stats_manager_map = get_stats_manager_map_callback_();
    for (const auto& [table, persistence_manager] : persistence_manager_map) {
        bool failed = false;
        if (!vector_store_map.contains(table)) {
            continue;
        }
        for (const auto& replica : replicas_) {
            // Retry the sync operation a constant kRetryCount number of times.
            for (int i = 0; i < kRetryCount; ++i) {
                vector_db::SyncRequest request;
                std::vector<vector_db::WALEntry> entries = persistence_manager->SerializeWALEntries(wal_offsets_per_replica_map_[table][replica]);
                if (entries.empty()) {
                    continue;
                }
                std::uint64_t checksum = vector_store_map.at(table)->ComputeChecksum();

                for (const auto& entry : entries) {
                    *request.add_entry() = entry;
                }

                auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
                vector_db::SyncResponse response;
                grpc::ClientContext context;
                grpc::Status status = stub->Sync(&context, request, &response);
                wal_offsets_per_replica_map_[table][replica] = wal_offsets_per_replica_map_[table][replica] + response.successful_count();
                if (status.ok() && response.successful_count() == entries.size()) {
                    // Compare the checksum of the replica store with the primary store to determine if they are in sync.
                    vector_db::GetChecksumRequest checksum_request;
                    checksum_request.set_table(table);
                    vector_db::GetChecksumResponse checksum_response;
                    grpc::ClientContext checksum_context;
                    grpc::Status checksum_status = stub->GetChecksum(&checksum_context, checksum_request, &checksum_response);
                    if (checksum_status.ok() && checksum == checksum_response.checksum()) {
                        break;
                    }
                    failed = true;
                    logger_->Error("replica out of sync: " + replica, name_);
                } else {
                    failed = true;
                    logger_->Error("error in updating replica: " + replica, name_);
                }
            }
        }
        // Only indicate the modified flag is false if all sync operations are successful.
        if (!failed) {
            if (!stats_manager_map.contains(table)) {
                continue;
            }
            stats_manager_map.at(table)->SetModifiedFlag(false);
        }
    }
}

bool ReplicaManager::ApplyWALEntry(const vector_db::WALEntry& entry) {
    return apply_callback_(entry);
}

std::uint64_t ReplicaManager::GetChecksum(std::string table) {
    const auto& vector_store_map = get_vector_store_map_callback_();
    if (!vector_store_map.contains(table)) {
        return 0;
    }
    return vector_store_map.at(table)->ComputeChecksum();
}

} // namespace vector_db_engine
