#include "engine.h"

#include <cstdint>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cleaner.h"
#include "flat_index.h"
#include "hnsw_index.h"
#include "local_logger.h"
#include "logger.h"
#include "metrics_manager.h"
#include "remote_logger.h"
#include "stats_manager.h"
#include "thread_pool.h"
#include "vector_store.h"

#include "replica_manager_service_impl.h"
#include "replica.grpc.pb.h"
#include "replica.pb.h"

namespace vector_db_engine {

constexpr int kShutdownCheckInterval = 1000;

inline const std::string kStoreSnapshotFilename = "store_snapshot.dat";
inline const std::string kIndexSnapshotFilename = "index_snapshot.dat";
inline const std::string kWriteAheadLogFilename = "wal.log";

Engine::Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string sync_server_address, std::vector<std::string> replicas)
    : shutdown_(false)
{
    metadata_ = Metadata{
        .name = name,
        .primary = primary,
        .sync_server_address = sync_server_address,
        .replicas = replicas,
        .reindex_threshold = reindex_threshold,
        .use_cache = use_cache,
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

    thread_pool_ = std::make_unique<ThreadPool>(std::thread::hardware_concurrency());
    logger_ = std::make_unique<LocalLogger>();

    replica_manager_ = std::make_unique<ReplicaManager>(metadata_.name, metadata_.primary, metadata_.sync_server_address, metadata_.replicas, shutdown_,
    [this](const vector_db::WALEntry& entry) {
        this->ApplyWALEntry(entry);
    },
    [this]() -> const auto& {
        return this->persistence_manager_map_;
    },
    logger_.get());
    cleaner_ = std::make_unique<Cleaner>(metadata_.name, shutdown_,
    [this](const std::string& table_name, bool force) {
        this->Cleanup(table_name, force);
    },
    [this]() -> const auto& {
        return this->store_map_;
    },
    [this]() -> const auto& {
     return this->stats_manager_map_;
    },
    logger_.get());

    for (const std::string& table : table_names) {
        HNSWIndex::HNSWIndexConfig hnsw_index_config = {16, 32, 64, 1.0f, vector_db_engine::VectorIndex::DistanceMetric::L2, 384};
        FlatIndex::FlatIndexConfig flat_index_config;
        bool ok = CreateTable(table, 384, hnsw_index_config, flat_index_config, 32);
        if (!ok) {
            continue;
        }
        std::unique_ptr<VectorStore> loaded_store = persistence_manager_map_[table]->LoadSnapshot();
        if (loaded_store) {
            store_map_[table] = std::move(loaded_store);
        }
        persistence_manager_map_[table]->ReplayWAL([this](const vector_db::WALEntry& entry) {
            this->ApplyWALEntry(entry);
        });
        stats_manager_map_[table]->Set(StatsManager::StatType::VECTOR, store_map_[table]->GetStore().size());
    }
}

Engine::~Engine() {
    shutdown_ = true;

    for (auto& [table, store] : store_map_) {
        if (!persistence_manager_map_.contains(table)) {
            continue;
        }
        Cleanup(table, true);
        replica_manager_->Sync(true);
        persistence_manager_map_[table]->SaveSnapshot(*store);
        persistence_manager_map_[table]->ClearWAL();
    }
}

bool Engine::Insert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return false;
    }

    persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

    bool ok = store_map_[table_name]->Insert(id, vector, content);
    if (ok) {
        stats_manager_map_[table_name]->Increment(StatsManager::StatType::VECTOR);
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::INSERT, 1);
    }
    return ok;
}

bool Engine::Remove(std::string table_name, Id id) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return false;
    }

    persistence_manager_map_[table_name]->AppendRemove(id);

    bool ok = store_map_[table_name]->Remove(id);
    if (ok) {
        stats_manager_map_[table_name]->SetRemovedFlag(true);
        stats_manager_map_[table_name]->Increment(StatsManager::StatType::DELETED);
        stats_manager_map_[table_name]->Increment(StatsManager::StatType::STALE);
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REMOVE, 1);
    }
    return ok;
}

std::vector<VectorStore::Data> Engine::Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return {};
    }

    auto start = std::chrono::steady_clock::now();
    std::vector<VectorStore::Data> data = store_map_[table_name]->Search(query, k, search_param);
    auto end = std::chrono::steady_clock::now();
    auto duration = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000;
    metrics_manager_map_[table_name]->CalculateSearchLatency(static_cast<float>(duration));
    metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::SEARCH, 1);

    return data;
}

std::vector<bool> Engine::BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return std::vector<bool>(vectors.size(), false);
    }

    std::vector<bool> success_flags;
    success_flags.reserve(vectors.size());
    for (const auto& [id, vector, content] : vectors) {
        persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

        bool ok = store_map_[table_name]->Insert(id, vector, content);
        if (ok) {
            stats_manager_map_[table_name]->Increment(StatsManager::StatType::VECTOR);
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::INSERT, 1);
        }
        success_flags.push_back(ok);
    }
    return success_flags;
}

std::vector<bool> Engine::BatchRemove(std::string table_name, std::vector<Id> ids) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return std::vector<bool>(ids.size(), false);
    }

    std::vector<bool> success_flags;
    success_flags.reserve(ids.size());
    for (const Id& id : ids) {
        persistence_manager_map_[table_name]->AppendRemove(id);

        bool ok = store_map_[table_name]->Remove(id);
        if (ok) {
            stats_manager_map_[table_name]->SetRemovedFlag(true);
            stats_manager_map_[table_name]->Increment(StatsManager::StatType::DELETED);
            stats_manager_map_[table_name]->Increment(StatsManager::StatType::STALE);
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REMOVE, 1);
        }
        success_flags.push_back(ok);
    }
    return success_flags;
}

std::vector<std::vector<VectorStore::Data>> Engine::BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || (!store_map_.contains(table_name) || !metrics_manager_map_.contains(table_name)) || !cache_map_.contains(table_name)) {
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

    if (metadata_.use_cache) {
        std::optional<Cache::CacheEntry> cache_entry = cache_map_[table_name]->Get(hash_key);
        if (cache_entry.has_value()) {
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::CACHE_HIT, 1);
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
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::CACHE_MISS, 1);
    }

    std::vector<std::future<std::vector<VectorStore::Data>>> futures;
    for (const auto& [query, k, search_param] : requests) {
        futures.push_back(thread_pool_->EnqueueTask([this, table_name, query, k, search_param]() {
            auto start = std::chrono::steady_clock::now();
            std::vector<VectorStore::Data> data = store_map_[table_name]->Search(query, k, search_param);
            auto end = std::chrono::steady_clock::now();
            auto duration = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) / 1000;
            metrics_manager_map_[table_name]->CalculateSearchLatency(static_cast<float>(duration));
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::SEARCH, 1);
            return data;
        }));
    }

    std::vector<std::vector<VectorStore::Data>> results;
    results.reserve(requests.size());
    for (auto& data_vector : futures) {
        results.push_back(data_vector.get());
    }

    if (metadata_.use_cache) {
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

StatsManager::Stats Engine::GetStats(std::string table_name) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !stats_manager_map_.contains(table_name)) {
        return {};
    }

    return stats_manager_map_[table_name]->GetStats();
}

MetricsManager::Metrics Engine::GetMetrics(std::string table_name) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || table_name.empty() || !metrics_manager_map_.contains(table_name)) {
        return {};
    }

    return metrics_manager_map_[table_name]->GetMetrics();
}

bool Engine::CreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    std::unique_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || name.empty()) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name) || stats_manager_map_.contains(name) || metrics_manager_map_.contains(name) || cache_map_.contains(name)) {
        return false;
    }

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;

    std::unique_ptr<VectorIndex> index = nullptr;
    VectorStore::IndexType index_type = VectorStore::IndexType::FLAT;
    VectorPersistenceManager::StoredIndexType stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;
    if (hnsw_index_config.m != 0 && hnsw_index_config.m0 != 0 && hnsw_index_config.ef_construction != 0 && hnsw_index_config.vector_dimensionality != 0) {
        index = std::make_unique<vector_db_engine::HNSWIndex>(hnsw_index_config);
        index_type = VectorStore::IndexType::HNSW;
        stored_index_type = VectorPersistenceManager::StoredIndexType::HNSW;
    } else {
        index = std::make_unique<vector_db_engine::FlatIndex>(flat_index_config);
        index_type = VectorStore::IndexType::FLAT;
        stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;
    }

    {
        auto temporary_persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, logger_.get());
        temporary_persistence_manager->AppendCreate(hnsw_index_config, flat_index_config, cache_size);
    }

    auto vector_store = std::make_unique<VectorStore>(index_type, std::move(index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, logger_.get());    auto metrics_manager = std::make_unique<MetricsManager>();
    auto stats_manager = std::make_unique<StatsManager>();
    auto lru_cache = std::make_unique<LRUCache>(cache_size);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map_[name] = std::move(persistence_manager);
    stats_manager_map_[name] = std::move(stats_manager);
    metrics_manager_map_[name] = std::move(metrics_manager);
    cache_map_[name] = std::move(lru_cache);

    return true;
}

bool Engine::CreateTableOnReplica(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    if (shutdown_ || name.empty()) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name) || stats_manager_map_.contains(name) || metrics_manager_map_.contains(name) || cache_map_.contains(name)) {
        return false;
    }

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;

    std::unique_ptr<VectorIndex> index = nullptr;
    VectorStore::IndexType index_type = VectorStore::IndexType::FLAT;
    VectorPersistenceManager::StoredIndexType stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;
    if (hnsw_index_config.m != 0 && hnsw_index_config.m0 != 0 && hnsw_index_config.ef_construction != 0 && hnsw_index_config.vector_dimensionality != 0) {
        index = std::make_unique<vector_db_engine::HNSWIndex>(hnsw_index_config);
        index_type = VectorStore::IndexType::HNSW;
        stored_index_type = VectorPersistenceManager::StoredIndexType::HNSW;
    } else {
        index = std::make_unique<vector_db_engine::FlatIndex>(flat_index_config);
        index_type = VectorStore::IndexType::FLAT;
        stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;
    }
    auto vector_store = std::make_unique<VectorStore>(index_type, std::move(index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, logger_.get());
    auto metrics_manager = std::make_unique<MetricsManager>();
    auto stats_manager = std::make_unique<StatsManager>();
    auto lru_cache = std::make_unique<LRUCache>(cache_size);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map_[name] = std::move(persistence_manager);
    stats_manager_map_[name] = std::move(stats_manager);
    metrics_manager_map_[name] = std::move(metrics_manager);
    cache_map_[name] = std::move(lru_cache);

    return true;
}

bool Engine::DropTable(std::string name) {
    std::unique_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || name.empty()) {
        return false;
    }

    if (!store_map_.contains(name) || !persistence_manager_map_.contains(name) || !stats_manager_map_.contains(name) || !metrics_manager_map_.contains(name) || !cache_map_.contains(name)) {
        return false;
    }

    persistence_manager_map_[name]->AppendDrop();
    replica_manager_->Sync(true);

    persistence_manager_map_[name]->Clear();

    store_map_.erase(name);
    persistence_manager_map_.erase(name);
    stats_manager_map_.erase(name);
    metrics_manager_map_.erase(name);
    cache_map_.erase(name);

    return true;
}

bool Engine::DropTableOnReplica(std::string name) {
    if (shutdown_ || name.empty()) {
        return false;
    }

    if (!store_map_.contains(name) || !persistence_manager_map_.contains(name) || !stats_manager_map_.contains(name) || !metrics_manager_map_.contains(name) || !cache_map_.contains(name)) {
        return false;
    }

    persistence_manager_map_[name]->Clear();

    store_map_.erase(name);
    persistence_manager_map_.erase(name);
    stats_manager_map_.erase(name);
    metrics_manager_map_.erase(name);
    cache_map_.erase(name);

    return true;
}

std::vector<std::string> Engine::ListTables() {
    std::shared_lock lock(engine_mutex_);
    std::vector<std::string> tables;
    for (const auto& [table, store] : store_map_) {
        tables.push_back(table);
    }
    return tables;
}

void Engine::Cleanup(const std::string& table_name, bool force) {
    std::unique_lock lock(engine_mutex_);
    std::unique_lock replica_lock(replica_mutex_);
    if ((shutdown_ && !force) || (!store_map_.contains(table_name)|| !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return;
    }

    StatsManager::Stats stats = stats_manager_map_[table_name]->GetStats();
    float ratio = 0;
    if (stats.vector_count + stats.stale_count - stats.deleted_count > 0) {
        ratio = static_cast<float>(stats.stale_count) / static_cast<float>(stats.vector_count + stats.stale_count - stats.deleted_count);
    }
    bool reindex = ratio > metadata_.reindex_threshold;

    store_map_[table_name]->Cleanup(reindex);
    metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::CLEANUP, 1);

    stats_manager_map_[table_name]->SetRemovedFlag(false);
    stats_manager_map_[table_name]->AdjustForDeletions();
    stats_manager_map_[table_name]->Reset(StatsManager::StatType::DELETED);

    if (reindex) {
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REINDEX, 1);
        stats_manager_map_[table_name]->Reset(StatsManager::StatType::STALE);
    }
}

void Engine::ApplyWALEntry(const vector_db::WALEntry& entry) {
    if (entry.type() == vector_db::WALEntry::CREATE) {
        const auto& config = entry.create_config();
        const auto& store_config = config.store_config();
        const auto& index_config = config.hnsw_index_config();
        const auto& cache_config = config.cache_config();

        bool ok = false;
        std::unique_lock replica_lock(replica_mutex_);
        HNSWIndex::HNSWIndexConfig hnsw_index_config = {index_config.m(), index_config.m0(), index_config.ef_construction(), index_config.ml(), (index_config.distance_metric() == vector_db::WALEntry::CreateConfig::L2 ? VectorIndex::DistanceMetric::L2 : VectorIndex::DistanceMetric::Cosine), store_config.vector_dimensionality()};
        FlatIndex::FlatIndexConfig flat_index_config;
        if (metadata_.primary) {
            ok = CreateTable(entry.table(), store_config.vector_dimensionality(), hnsw_index_config, flat_index_config, cache_config.cache_size());
        } else {
            ok = CreateTableOnReplica(entry.table(), store_config.vector_dimensionality(), hnsw_index_config, flat_index_config, cache_config.cache_size());
        }
    }
    if (entry.type() == vector_db::WALEntry::DROP) {
        const auto& config = entry.drop_config();
        bool ok = false;
        std::unique_lock replica_lock(replica_mutex_);
        if (metadata_.primary) {
            ok = DropTable(entry.table());
        } else {
            ok = DropTableOnReplica(entry.table());
        }
    }

    std::unique_lock lock(engine_mutex_);
    if (!store_map_.contains(entry.table()) || !stats_manager_map_.contains(entry.table())) {
        return;
    }
    if (entry.type() == vector_db::WALEntry::INSERT) {
        const auto& config = entry.insert_config();
        Vector vector(config.vector().begin(), config.vector().end());
        std::unique_lock replica_lock(replica_mutex_);
        bool ok = store_map_[entry.table()]->Insert(config.id(), vector, config.content());
        if (ok) {
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::VECTOR);
        }
    }
    if (entry.type() == vector_db::WALEntry::REMOVE) {
        const auto& config = entry.remove_config();
        std::unique_lock replica_lock(replica_mutex_);
        bool ok = store_map_[entry.table()]->Remove(config.id());
        if (ok) {
            stats_manager_map_[entry.table()]->SetRemovedFlag(true);
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::DELETED);
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::STALE);
        }
    }
}

Logger* Engine::GetLogger() const {
    return logger_.get();
}

} // namespace vector_db_engine
