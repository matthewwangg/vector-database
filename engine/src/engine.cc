#include "engine.h"

#include <cstdint>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "vector_store.h"

namespace vector_db_engine {

constexpr int kCleanupInterval = 60;

inline const std::string kStoreSnapshotFilename = "store_snapshot.dat";
inline const std::string kIndexSnapshotFilename = "index_snapshot.dat";
inline const std::string kWriteAheadLogFilename = "wal.log";

Engine::Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality, float reindex_threshold)
    : store_(std::make_unique<VectorStore>(std::move(index), vector_dimensionality)),
      persistence_manager_(std::make_unique<VectorPersistenceManager>(kStoreSnapshotFilename, kIndexSnapshotFilename, kWriteAheadLogFilename)),
      reindex_threshold_(reindex_threshold),
      shutdown_(false),
      removed_(false)
{
    std::unique_ptr<VectorStore> loaded_store = persistence_manager_->LoadSnapshot();
    if (loaded_store) {
        persistence_manager_->ReplayWAL(*loaded_store);
        store_ = std::move(loaded_store);
    } else {
        persistence_manager_->ReplayWAL(*store_);
    }

    stats_.vector_count = store_->GetStore().size();
    cleanup_thread_ = std::thread(&Engine::BackgroundCleanupLoop, this);
}

Engine::~Engine() {
    shutdown_ = true;
    cleanup_cv_.notify_one();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    Cleanup(true);
    persistence_manager_->SaveSnapshot(*store_);
    persistence_manager_->ClearWAL();
}

bool Engine::Insert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
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
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_map_.contains(table_name) || !metrics_map_.contains(table_name))) {
        return false;
    }

    persistence_manager_map_[table_name]->AppendRemove(id);

    bool ok = store_map_[table_name]->Remove(id);
    if (ok) {
        removed_ = true;
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
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    metrics_map_[table_name].average_search_latency_ms = (metrics_map_[table_name].average_search_latency_ms * metrics_map_[table_name].search_count + duration) / (metrics_map_[table_name].search_count + 1);
    metrics_map_[table_name].search_count++;

    return data;
}

Engine::Stats Engine::GetStats(std::string table_name) const {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !stats_map_.contains(table_name)) {
        return {};
    }

    return stats_map_[table_name];
}

Engine::Metrics Engine::GetMetrics(std::string table_name) const {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !metrics_map_.contains(table_name)) {
        return {};
    }

    return metrics_map_[table_name];
};

bool Engine::CreateTable(std::string name) {
    std::unique_lock lock(engine_mutex_);
    if (shutdown_ || name.empty()) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name)) {
        return false;
    }

    std::size_t m = 16;
    std::size_t m0 = 32;
    std::size_t ef_construction = 64;
    float ml = 1.0f;
    int vector_dimensionality = 384;
    auto distance_metric = vector_db_engine::HNSWIndex::DistanceMetric::L2;

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;

    auto hnsw_index = std::make_unique<vector_db_engine::HNSWIndex>(m, m0, ef_construction, ml, distance_metric, vector_dimensionality);
    auto vector_store = std::make_unique<VectorStore>(std::move(hnsw_index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(store_snapshot, index_snapshot, write_ahead_log);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map[name] = std::move(persistence_manager);
    stats_map_[name] = {};
    metrics_map_[name] = {};
    removed_flag_map_[name] = false;

    return true;
}

void Engine::BackgroundCleanupLoop() {
    std::unique_lock<std::mutex> lock(cleanup_mutex_);
    while (!shutdown_) {
        cleanup_cv_.wait_for(lock, std::chrono::seconds(kCleanupInterval));

        if (shutdown_) {
            break;
        }

        if (removed_) {
            auto start = std::chrono::steady_clock::now();
            Cleanup(false);
            auto end = std::chrono::steady_clock::now();
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << "cleanup completed in " << duration_ms << " ms\n";
        }
    }
}

void Engine::Cleanup(std::string table_name, bool force) {
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

    removed_ = false;
    stats_map_[table_name].vector_count = stats_map_[table_name].vector_count - stats_map_[table_name].deleted_count;
    stats_map_[table_name].deleted_count = 0;

    if (reindex) {
        metrics_map_[table_name].reindex_count++;
        stats_map_[table_name].stale_count = 0;
    }
}

} // namespace vector_db_engine
