#include "engine.h"

#include <cstdint>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "local_logger.h"
#include "hnsw_index.h"
#include "thread_pool.h"
#include "vector_store.h"

#include "replica_manager_service_impl.h"
#include "replica.grpc.pb.h"
#include "replica.pb.h"

namespace vector_db_engine {

constexpr int kCleanupInterval = 60;
constexpr int kSyncInterval = 90;
constexpr int kShutdownCheckInterval = 1000;

inline const std::string kStoreSnapshotFilename = "store_snapshot.dat";
inline const std::string kIndexSnapshotFilename = "index_snapshot.dat";
inline const std::string kWriteAheadLogFilename = "wal.log";

Engine::Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string sync_server_address, std::vector<std::string> replicas)
    : reindex_threshold_(reindex_threshold),
      use_cache_(use_cache),
      replicas_(replicas),
      shutdown_(false)
{
    metadata_ = Metadata{
        .name = name,
        .primary = primary,
        .sync_server_address = sync_server_address
    };

    std::vector<std::string> table_names = [&]() {
        std::vector<std::string> tables;
        const std::string suffix = "_" + kWriteAheadLogFilename;

        const std::string base_directory = std::string(std::getenv("HOME")) + "/.vector_db/" + metadata_.name;
        std::filesystem::create_directories(base_directory);
        for (const auto& entry : std::filesystem::directory_iterator(base_directory)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.size() > suffix.size() && filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) == 0) {
                    tables.push_back(filename.substr(0, filename.size() - suffix.size()));
                }
            }
        }
        return tables;
    }();

    for (const std::string& table : table_names) {
        bool ok = CreateTable(table);
        if (!ok) {
            continue;
        }
        std::unique_ptr<VectorStore> loaded_store = persistence_manager_map_[table]->LoadSnapshot();
        if (loaded_store) {
            persistence_manager_map_[table]->ReplayWAL(*loaded_store);
            store_map_[table] = std::move(loaded_store);
        } else {
            persistence_manager_map_[table]->ReplayWAL(*store_map_[table]);
        }

        stats_map_[table].vector_count = store_map_[table]->GetStore().size();
    }

    thread_pool_ = std::make_unique<ThreadPool>(std::thread::hardware_concurrency());
    logger_ = std::make_unique<LocalLogger>();

    cleanup_thread_ = std::thread(&Engine::BackgroundCleanupLoop, this);
    if (metadata_.primary && !replicas.empty()) {
        sync_thread_ = std::thread(&Engine::BackgroundSyncReplicasLoop, this);
    } else if (!metadata_.primary) {
        sync_thread_ = std::thread(&Engine::BackgroundWaitForSyncLoop, this);
    }
}

Engine::~Engine() {
    shutdown_ = true;
    if (sync_thread_.joinable()) {
        sync_thread_.join();
    }

    cleanup_cv_.notify_one();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }

    for (auto& [table, store] : store_map_) {
        if (!persistence_manager_map_.contains(table)) {
            continue;
        }
        Cleanup(table, true);
        Sync(true);
        persistence_manager_map_[table]->SaveSnapshot(*store);
        persistence_manager_map_[table]->ClearWAL();
    }
}

bool Engine::Insert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return false;
    }

    persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

    bool ok = store_map_[table_name]->Insert(id, vector, content);
    if (ok) {
        stats_map_[table_name].vector_count++;
        metrics_map_[table_name].insert_count++;
    }
    return ok;
}

bool Engine::Remove(std::string table_name, Id id) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return false;
    }

    persistence_manager_map_[table_name]->AppendRemove(id);

    bool ok = store_map_[table_name]->Remove(id);
    if (ok) {
        removed_flag_map_[table_name] = true;
        stats_map_[table_name].deleted_count++;
        stats_map_[table_name].stale_count++;
        metrics_map_[table_name].remove_count++;
    }
    return ok;
}

std::vector<VectorStore::Data> Engine::Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return {};
    }

    auto start = std::chrono::steady_clock::now();
    std::vector<VectorStore::Data> data = store_map_[table_name]->Search(query, k, search_param);
    auto end = std::chrono::steady_clock::now();
    auto duration = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000;
    metrics_map_[table_name].average_search_latency_ms = (metrics_map_[table_name].average_search_latency_ms * metrics_map_[table_name].search_count + duration) / (metrics_map_[table_name].search_count + 1);
    metrics_map_[table_name].search_count++;

    return data;
}

std::vector<bool> Engine::BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return std::vector<bool>(vectors.size(), false);
    }

    std::vector<bool> success_flags;
    success_flags.reserve(vectors.size());
    for (const auto& [id, vector, content] : vectors) {
        persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

        bool ok = store_map_[table_name]->Insert(id, vector, content);
        if (ok) {
            stats_map_[table_name].vector_count++;
            metrics_map_[table_name].insert_count++;
        }
        success_flags.push_back(ok);
    }
    return success_flags;
}

std::vector<bool> Engine::BatchRemove(std::string table_name, std::vector<Id> ids) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return std::vector<bool>(ids.size(), false);
    }

    std::vector<bool> success_flags;
    success_flags.reserve(ids.size());
    for (const Id& id : ids) {
        persistence_manager_map_[table_name]->AppendRemove(id);

        bool ok = store_map_[table_name]->Remove(id);
        if (ok) {
            removed_flag_map_[table_name] = true;
            stats_map_[table_name].deleted_count++;
            stats_map_[table_name].stale_count++;
            metrics_map_[table_name].remove_count++;
        }
        success_flags.push_back(ok);
    }
    return success_flags;
}

std::vector<std::vector<VectorStore::Data>> Engine::BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !metrics_map_.contains(table_name)) || !cache_map_.contains(table_name)) {
        return {};
    }

    auto hash_key = [&](){
        std::size_t hash = requests.size();
        for (const auto& [vector, k, search_param] : requests) {
            hash ^= k;
            hash ^= search_param;
            for (std::size_t i = 0; i < vector.size(); i += 2) {
                hash ^= std::hash<float>{}(vector[i]);
            }
        }
        return hash;
    }();

    if (use_cache_) {
        std::optional<Cache::CacheEntry> cache_entry = cache_map_[table_name]->Get(hash_key);
        if (cache_entry.has_value()) {
            metrics_map_[table_name].cache_hit++;
            return [&]() {
                std::vector<std::vector<VectorStore::Data>> results;
                results.reserve(cache_entry->data.size());
                for (const auto& group : cache_entry->data) {
                    std::vector<VectorStore::Data> result_group;
                    for (const auto& data : group) {
                        result_group.emplace_back(VectorStore::Data{data.id, data.vector, data.content});
                    }
                    results.emplace_back(std::move(result_group));
                }
                return results;
            }();
        }
        metrics_map_[table_name].cache_miss++;
    }

    std::vector<std::future<std::vector<VectorStore::Data>>> futures;
    for (const auto& [query, k, search_param] : requests) {
        futures.push_back(thread_pool_->EnqueueTask([this, table_name, query, k, search_param]() {
            auto start = std::chrono::steady_clock::now();
            std::vector<VectorStore::Data> data = store_map_[table_name]->Search(query, k, search_param);
            auto end = std::chrono::steady_clock::now();
            auto duration = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000;
            metrics_map_[table_name].min_search_latency_ms = std::min(metrics_map_[table_name].min_search_latency_ms, static_cast<float>(duration));
            metrics_map_[table_name].max_search_latency_ms = std::max(metrics_map_[table_name].max_search_latency_ms, static_cast<float>(duration));
            metrics_map_[table_name].average_search_latency_ms = (metrics_map_[table_name].average_search_latency_ms * metrics_map_[table_name].search_count + duration) / (metrics_map_[table_name].search_count + 1);
            metrics_map_[table_name].search_count++;
            return data;
        }));
    }

    std::vector<std::vector<VectorStore::Data>> results;
    results.reserve(requests.size());
    for (auto& data_vector : futures) {
        results.push_back(data_vector.get());
    }

    if (use_cache_) {
        Cache::CacheEntry new_entry = [&]() {
            Cache::CacheEntry entry;
            entry.data.reserve(results.size());
            for (const auto& result_group : results) {
                std::vector<Cache::CacheEntry::Data> data_group;
                data_group.reserve(result_group.size());
                for (const auto& data : result_group) {
                    data_group.emplace_back(Cache::CacheEntry::Data{data.id, data.vector, data.content});
                }
                entry.data.emplace_back(data_group);
            }
            return entry;
        }();
        cache_map_[table_name]->Store(hash_key, new_entry);
    }

    return results;
}

Engine::Stats Engine::GetStats(std::string table_name) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !stats_map_.contains(table_name)) {
        return {};
    }

    return stats_map_[table_name];
}

Engine::Metrics Engine::GetMetrics(std::string table_name) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !metrics_map_.contains(table_name)) {
        return {};
    }

    return metrics_map_[table_name];
}

bool Engine::CreateTable(std::string name) {
    std::unique_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || name.empty()) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name) || stats_map_.contains(name) || metrics_map_.contains(name) || removed_flag_map_.contains(name) || cache_map_.contains(name)) {
        return false;
    }

    std::size_t m = 16;
    std::size_t m0 = 32;
    std::size_t ef_construction = 64;
    float ml = 1.0f;
    int vector_dimensionality = 384;
    auto distance_metric = vector_db_engine::HNSWIndex::DistanceMetric::L2;

    std::size_t cache_size = 32;

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;

    auto hnsw_index = std::make_unique<vector_db_engine::HNSWIndex>(m, m0, ef_construction, ml, distance_metric, vector_dimensionality);
    auto vector_store = std::make_unique<VectorStore>(std::move(hnsw_index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, store_snapshot, index_snapshot, write_ahead_log);
    auto lru_cache = std::make_unique<LRUCache>(cache_size);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map_[name] = std::move(persistence_manager);
    stats_map_[name] = {};
    metrics_map_[name] = {};
    removed_flag_map_[name] = false;
    cache_map_[name] = std::move(lru_cache);
    replica_wal_offsets_map_[name] = 0;

    return true;
}

bool Engine::CreateTableWithoutLock(std::string name) {
    if (shutdown_ || name.empty()) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name) || stats_map_.contains(name) || metrics_map_.contains(name) || removed_flag_map_.contains(name) || cache_map_.contains(name)) {
        return false;
    }

    std::size_t m = 16;
    std::size_t m0 = 32;
    std::size_t ef_construction = 64;
    float ml = 1.0f;
    int vector_dimensionality = 384;
    auto distance_metric = vector_db_engine::HNSWIndex::DistanceMetric::L2;

    std::size_t cache_size = 32;

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;

    auto hnsw_index = std::make_unique<vector_db_engine::HNSWIndex>(m, m0, ef_construction, ml, distance_metric, vector_dimensionality);
    auto vector_store = std::make_unique<VectorStore>(std::move(hnsw_index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, store_snapshot, index_snapshot, write_ahead_log);
    auto lru_cache = std::make_unique<LRUCache>(cache_size);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map_[name] = std::move(persistence_manager);
    stats_map_[name] = {};
    metrics_map_[name] = {};
    removed_flag_map_[name] = false;
    cache_map_[name] = std::move(lru_cache);
    replica_wal_offsets_map_[name] = 0;

    return true;
}

bool Engine::DropTable(std::string name) {
    std::unique_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || name.empty()) {
        return false;
    }

    if (!store_map_.contains(name) || !persistence_manager_map_.contains(name) || !stats_map_.contains(name) || !metrics_map_.contains(name) || !removed_flag_map_.contains(name) || !cache_map_.contains(name)) {
        return false;
    }

    persistence_manager_map_[name]->Clear();

    store_map_.erase(name);
    persistence_manager_map_.erase(name);
    stats_map_.erase(name);
    metrics_map_.erase(name);
    removed_flag_map_.erase(name);
    cache_map_.erase(name);
    replica_wal_offsets_map_.erase(name);

    vector_db::DropRequest request;
    request.set_table(name);
    for (const auto& replica : replicas_) {
        auto stub = vector_db::ReplicaManager::NewStub(grpc::CreateChannel(replica, grpc::InsecureChannelCredentials()));
        vector_db::DropResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub->Drop(&context, request, &response);
    }

    return true;
}

bool Engine::DropTableWithoutLock(std::string name) {
    if (shutdown_ || name.empty()) {
        return false;
    }

    if (!store_map_.contains(name) || !persistence_manager_map_.contains(name) || !stats_map_.contains(name) || !metrics_map_.contains(name) || !removed_flag_map_.contains(name) || !cache_map_.contains(name)) {
        return false;
    }

    persistence_manager_map_[name]->Clear();

    store_map_.erase(name);
    persistence_manager_map_.erase(name);
    stats_map_.erase(name);
    metrics_map_.erase(name);
    removed_flag_map_.erase(name);
    cache_map_.erase(name);
    replica_wal_offsets_map_.erase(name);

    return true;
}

void Engine::BackgroundCleanupLoop() {
    std::unique_lock<std::mutex> lock(cleanup_mutex_);
    while (!shutdown_) {
        cleanup_cv_.wait_for(lock, std::chrono::seconds(kCleanupInterval));

        if (shutdown_) {
            break;
        }

        for (auto& [table, store] : store_map_) {
            if (removed_flag_map_.contains(table) && removed_flag_map_[table]) {
                auto start = std::chrono::steady_clock::now();
                Cleanup(table, false);
                auto end = std::chrono::steady_clock::now();
                auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                std::cout << "cleanup completed in " << duration_ms << " ms\n";
                removed_flag_map_[table] = false;
            }
        }
    }
}

void Engine::Cleanup(const std::string& table_name, bool force) {
    std::shared_lock lock(engine_mutex_);
    if ((shutdown_ && !force) || (!store_map_.contains(table_name)|| !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return;
    }

    float ratio = 0;
    if (stats_map_[table_name].vector_count + stats_map_[table_name].stale_count - stats_map_[table_name].deleted_count > 0) {
        ratio = static_cast<float>(stats_map_[table_name].stale_count) / static_cast<float>(stats_map_[table_name].vector_count + stats_map_[table_name].stale_count - stats_map_[table_name].deleted_count);
    }
    bool reindex = ratio > reindex_threshold_;

    store_map_[table_name]->Cleanup(reindex);
    metrics_map_[table_name].cleanup_count++;

    removed_flag_map_[table_name] = false;
    stats_map_[table_name].vector_count = stats_map_[table_name].vector_count - stats_map_[table_name].deleted_count;
    stats_map_[table_name].deleted_count = 0;

    if (reindex) {
        metrics_map_[table_name].reindex_count++;
        stats_map_[table_name].stale_count = 0;
    }
}

void Engine::BackgroundSyncReplicasLoop() {
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
        std::cout << "sync completed in " << duration_ms << " ms" << std::endl;
    }
}

void Engine::Sync(bool force) {
    std::shared_lock lock(engine_mutex_);
    if ((shutdown_ && !force)) {
        return;
    }

    for (const auto& [table, persistence_manager] : persistence_manager_map_) {
        vector_db::SyncRequest request;
        std::vector<vector_db::WALEntry> entries = persistence_manager_map_[table]->SerializeWALEntries(replica_wal_offsets_map_[table]);
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
                std::cout << "error in updating replica: " << replica << std::endl;
            }
        }

        replica_wal_offsets_map_[table] += entries.size();
    }
}

void Engine::BackgroundWaitForSyncLoop() {
    if (metadata_.sync_server_address.empty()) {
        std::cout << "no sync server address specified" << std::endl;
        return;
    }

    ReplicaManagerServiceImpl replica_manager_service(this);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(metadata_.sync_server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&replica_manager_service);

    std::cout << "replica sync server running on " << metadata_.sync_server_address << std::endl;
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    std::thread shutdown_thread([&server, this]() {
        while (!shutdown_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckInterval));
        }
        std::cout << "replica sync server shutting down..." << std::endl;
        server->Shutdown();
    });

    server->Wait();
    shutdown_thread.join();
}

void Engine::ApplyWALEntry(const vector_db::WALEntry& entry) {
    std::unique_lock lock(engine_mutex_);
    if (!store_map_.contains(entry.table()) || !stats_map_.contains(entry.table()) || !removed_flag_map_.contains(entry.table())) {
        bool ok = CreateTableWithoutLock(entry.table());
        if (!ok) {
            return;
        }
    }
    if (entry.type() == vector_db::WALEntry::INSERT) {
        Vector vector(entry.vector().begin(), entry.vector().end());
        bool ok = store_map_[entry.table()]->Insert(entry.id(), vector, entry.content());
        if (ok) {
            stats_map_[entry.table()].vector_count++;
        }
    }
    if (entry.type() == vector_db::WALEntry::REMOVE) {
        bool ok = store_map_[entry.table()]->Remove(entry.id());
        if (ok) {
            removed_flag_map_[entry.table()] = true;
            stats_map_[entry.table()].deleted_count++;
            stats_map_[entry.table()].stale_count++;
        }
    }
}

} // namespace vector_db_engine
