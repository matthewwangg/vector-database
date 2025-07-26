#include "replica_manager.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "engine.h"

#include "replica_manager_service_impl.h"
#include "replica.pb.h"

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
{
    if (primary_ && !replicas_.empty()) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaSyncLoop, this);
    } else if (!primary_) {
        sync_thread_ = std::thread(&ReplicaManager::RunReplicaServer, this);
    }
}

ReplicaManager::~ReplicaManager() {
    if (sync_thread_.joinable()) {
        sync_thread_.join();
    }
}

void ReplicaManager::RunReplicaServer() {
    if (sync_server_address_.empty()) {
        engine_->GetLogger()->Warn("no sync server address specified", name_);
        return;
    }

    ReplicaManagerServiceImpl replica_manager_service(this);

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
        Sync(false);
        auto end = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        engine_->GetLogger()->Info("sync completed in " + std::to_string(duration_ms) + " ms", name_);
    }
}

void ReplicaManager::Sync(bool force) {
    if ((shutdown_ && !force)) {
        return;
    }

    const auto& persistence_manager_map = engine_->GetPersistenceManagerMap();
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
            auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
            vector_db::SyncResponse response;
            grpc::ClientContext context;
            grpc::Status status = stub->Sync(&context, request, &response);
            if (!status.ok()) {
                engine_->GetLogger()->Error("error in updating replica: " + replica, name_);
            }
        }

        wal_offsets_map_[table] += entries.size();
    }
}

void ReplicaManager::SyncCreateTable(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size) {
    vector_db::CreateRequest request;
    request.set_table(name);
    request.mutable_store_config()->set_vector_dimensionality(vector_dimensionality);
    request.mutable_hnsw_index_config()->set_m(m);
    request.mutable_hnsw_index_config()->set_m0(m0);
    request.mutable_hnsw_index_config()->set_ef_construction(ef_construction);
    request.mutable_hnsw_index_config()->set_ml(ml);
    if (distance_metric == HNSWIndex::DistanceMetric::L2) {
        request.mutable_hnsw_index_config()->set_distance_metric(vector_db::CreateRequest_HNSWIndexConfig_DistanceMetric_L2);
    } else {
        request.mutable_hnsw_index_config()->set_distance_metric(vector_db::CreateRequest_HNSWIndexConfig_DistanceMetric_COSINE);
    }
    request.mutable_cache_config()->set_cache_size(cache_size);

    for (const auto& replica : replicas_) {
        auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
        vector_db::CreateResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub->Create(&context, request, &response);
    }
}

void ReplicaManager::SyncDropTable(std::string name) {
    vector_db::DropRequest request;
    request.set_table(name);
    for (const auto& replica : replicas_) {
        auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
        vector_db::DropResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub->Drop(&context, request, &response);
    }
}

void ReplicaManager::ApplyWALEntry(const vector_db::WALEntry& entry) {
    engine_->ApplyWALEntry(entry);
}

bool ReplicaManager::CreateTableOnReplica(std::string name, int vector_dimensionality, std::size_t m, std::size_t m0, std::size_t ef_construction, float ml, vector_db_engine::HNSWIndex::DistanceMetric distance_metric, std::size_t cache_size) {
    return engine_->CreateTableOnReplica(name, vector_dimensionality, m, m0, ef_construction, ml, distance_metric, cache_size);
}

bool ReplicaManager::DropTableOnReplica(std::string name) {
    return engine_->DropTableOnReplica(name);
}

} // namespace vector_db_engine
