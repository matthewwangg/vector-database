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
#include "input_validator.h"
#include "local_logger.h"
#include "logger.h"
#include "metrics_manager.h"
#include "remote_logger.h"
#include "silent_logger.h"
#include "stats_manager.h"
#include "thread_pool.h"
#include "vector_store.h"

#include "replica_manager_service_impl.h"
#include "replica.grpc.pb.h"
#include "replica.pb.h"
#include "storage.pb.h"

namespace vector_db_engine {

constexpr int kShutdownCheckInterval = 1000;

inline const std::string kStoreSnapshotFilename = "store_snapshot.dat";
inline const std::string kIndexSnapshotFilename = "index_snapshot.dat";
inline const std::string kWriteAheadLogFilename = "wal.log";
inline const std::string kMetadataFilename = "metadata.bin";

Engine::Engine(std::string name, bool primary, float reindex_threshold, bool use_cache, std::string sync_server_address, std::vector<std::string> replicas, int cleanup_interval, int sync_interval, Metadata::LoggerType logger_type)
    : shutdown_(false)
{
    metadata_ = Metadata{
        .name = name,
        .primary = primary,
        .sync_server_address = sync_server_address,
        .replicas = replicas,
        .reindex_threshold = reindex_threshold,
        .use_cache = use_cache,
        .cleanup_interval = cleanup_interval,
        .sync_interval = sync_interval,
        .logger_type = logger_type,
    };

    auto apply_wal_entry = [this](const vector_db::WALEntry& entry) {
        return this->ApplyWALEntry(entry);
    };
    auto cleanup = [this](const std::string& table_name, bool force) {
        this->Cleanup(table_name, force);
    };
    auto get_persistence_manager_map = [this]() -> const auto& {
        return this->persistence_manager_map_;
    };
    auto get_stats_manager_map = [this]() -> const auto& {
        return this->stats_manager_map_;
    };
    auto get_store_map = [this]() -> const auto& {
        return this->store_map_;
    };

    if (metadata_.logger_type == Metadata::LoggerType::LOCAL) {
        logger_ = std::make_unique<LocalLogger>();
    } else if (metadata_.logger_type == Metadata::LoggerType::REMOTE) {
        logger_ = std::make_unique<RemoteLogger>("0.0.0.0:50051");
    } else {
        logger_ = std::make_unique<SilentLogger>();
    }

    thread_pool_ = std::make_unique<ThreadPool>(std::thread::hardware_concurrency());
    input_validator_ = std::make_unique<InputValidator>();
    replica_manager_ = std::make_unique<ReplicaManager>(metadata_.name, metadata_.primary, metadata_.sync_server_address, metadata_.replicas, metadata_.sync_interval, shutdown_, apply_wal_entry, get_persistence_manager_map, get_stats_manager_map, get_store_map, logger_.get());
    cleaner_ = std::make_unique<Cleaner>(metadata_.cleanup_interval, shutdown_, cleanup, get_store_map, get_stats_manager_map);

    // Discover tables to restore by scanning for WAL files, empty or not.
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
        int vector_dimensionality;
        std::size_t cache_size;
        HNSWIndex::HNSWIndexConfig hnsw_index_config = {};
        FlatIndex::FlatIndexConfig flat_index_config = {};

        // Load all table metadata from metadata.bin files.
        [&]() {
            std::string filepath = std::string(std::getenv("HOME")) + "/.vector_db/" + metadata_.name + "/" + table + "_" + kMetadataFilename;
            std::ifstream in(filepath, std::ios::binary);
            if (!in) {
                return;
            }

            vector_db::TableMetadata table_metadata;
            table_metadata.ParseFromIstream(&in);

            vector_dimensionality = table_metadata.vector_dimensionality();
            cache_size = table_metadata.cache_size();

            if (table_metadata.index_type() == vector_db::TableMetadata::HNSW) {
                vector_db::TableMetadata::HNSWIndexConfig config = table_metadata.hnsw_index_config();
                hnsw_index_config = {config.m(), config.m0(), config.ef_construction(), config.ml(), static_cast<VectorIndex::DistanceMetric>(config.distance_metric()), config.vector_dimensionality()};
            } else {
                vector_db::TableMetadata::FlatIndexConfig config = table_metadata.flat_index_config();
                flat_index_config = {config.vector_dimensionality(), static_cast<VectorIndex::DistanceMetric>(config.distance_metric())};
            }
        }();

        bool ok = CreateTable(table, vector_dimensionality, hnsw_index_config, flat_index_config, cache_size);
        if (!ok) {
            continue;
        }

        // Load the snapshot of the table if it exists.
        std::unique_ptr<VectorStore> loaded_store = persistence_manager_map_[table]->LoadSnapshot();
        if (loaded_store) {
            store_map_[table] = std::move(loaded_store);
        }

        // On startup after a crash, replay the WAL to ensure durability.
        persistence_manager_map_[table]->ReplayWAL(apply_wal_entry);
        stats_manager_map_[table]->Set(StatsManager::StatType::VECTOR, store_map_[table]->GetStore().size());
    }
}

Engine::~Engine() {
    shutdown_ = true;

    for (auto& [table, store] : store_map_) {
        if (!persistence_manager_map_.contains(table)) {
            continue;
        }

        // Ensure saved indexes are clean as existing soft removed nodes won't be detected on startup.
        Cleanup(table, true);

        // Ensure replicas are all in sync before the WAL is cleared. This implies that currently, the primary should be shut down before the replicas, unless all replicas are already in sync.
        replica_manager_->Sync(true);

        // Ensure tables can be reloaded on startup.
        persistence_manager_map_[table]->SaveSnapshot(*store);
        persistence_manager_map_[table]->SaveMetadata(store->GetVectorDimensionality(), (store->GetIndexType() == VectorStore::IndexType::HNSW ? dynamic_cast<const HNSWIndex*>(store->GetIndex())->GetConfig() : HNSWIndex::HNSWIndexConfig{}), (store->GetIndexType() == VectorStore::IndexType::FLAT ? dynamic_cast<const FlatIndex*>(store->GetIndex())->GetConfig() : FlatIndex::FlatIndexConfig{}), (cache_map_.contains(table) ? dynamic_cast<const LRUCache*>(cache_map_.at(table).get())->GetMaxCacheSize() : 32));

        // Clear WAL as state has been saved, so it is no longer required for durability.
        persistence_manager_map_[table]->ClearWAL();
    }
}

bool Engine::Insert(std::string table_name, Id id, const Vector& vector, const std::string& content) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return false;
    }

    if (!input_validator_->ValidateInsert(table_name, id, vector, content)) {
        return false;
    }

    // Update the WAL ahead of time for durability and replay for primary and replicas.
    persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

    bool ok = store_map_[table_name]->Insert(id, vector, content);
    if (ok) {
        // Indicate the table has been modified to trigger sync on all replicas.
        stats_manager_map_[table_name]->SetModifiedFlag(true);

        stats_manager_map_[table_name]->Increment(StatsManager::StatType::VECTOR);
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::INSERT, 1);
    }

    if (metadata_.use_cache && cache_map_.contains(table_name) && stats_manager_map_[table_name]->GetModifiedFlag()) {
        cache_map_[table_name]->InvalidateAll();
    }

    return ok;
}

bool Engine::Remove(std::string table_name, Id id) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return false;
    }

    if (!input_validator_->ValidateRemove(table_name, id)) {
        return false;
    }

    // Update the WAL ahead of time for durability and replay for primary and replicas.
    persistence_manager_map_[table_name]->AppendRemove(id);

    bool ok = store_map_[table_name]->Remove(id);
    if (ok) {
        // Indicate the table has been modified to trigger sync on all replicas.
        stats_manager_map_[table_name]->SetModifiedFlag(true);

        // Indicate the table has had a vector removed to trigger cleanup on HNSW index.
        stats_manager_map_[table_name]->SetRemovedFlag(true);

        stats_manager_map_[table_name]->Increment(StatsManager::StatType::DELETED);
        stats_manager_map_[table_name]->Increment(StatsManager::StatType::STALE);
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REMOVE, 1);
    }

    if (metadata_.use_cache && cache_map_.contains(table_name) && stats_manager_map_[table_name]->GetModifiedFlag()) {
        cache_map_[table_name]->InvalidateAll();
    }

    return ok;
}

std::vector<VectorStore::Data> Engine::Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || (!store_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return {};
    }

    if (!input_validator_->ValidateSearch(table_name, query, k, search_param)) {
        return {};
    }

    // Measure search latency for metrics calculations.
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
    if (!metadata_.primary || shutdown_ || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return std::vector<bool>(vectors.size(), false);
    }

    for (const auto& [id, vector, content] : vectors) {
        if (!input_validator_->ValidateInsert(table_name, id, vector, content)) {
            return std::vector<bool>(vectors.size(), false);
        }
    }

    std::vector<bool> success_flags;
    success_flags.reserve(vectors.size());
    for (const auto& [id, vector, content] : vectors) {
        // Update the WAL ahead of time for durability and replay for primary and replicas.
        persistence_manager_map_[table_name]->AppendInsert(id, vector, content);

        bool ok = store_map_[table_name]->Insert(id, vector, content);
        if (ok) {
            stats_manager_map_[table_name]->SetModifiedFlag(true);
            stats_manager_map_[table_name]->Increment(StatsManager::StatType::VECTOR);
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::INSERT, 1);
        }
        success_flags.push_back(ok);
    }

    if (metadata_.use_cache && cache_map_.contains(table_name) && stats_manager_map_[table_name]->GetModifiedFlag()) {
        cache_map_[table_name]->InvalidateAll();
    }

    return success_flags;
}

std::vector<bool> Engine::BatchRemove(std::string table_name, std::vector<Id> ids) {
    std::shared_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_ || (!store_map_.contains(table_name) || !persistence_manager_map_.contains(table_name) || !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return std::vector<bool>(ids.size(), false);
    }

    for (auto id : ids) {
        if (!input_validator_->ValidateRemove(table_name, id)) {
            return std::vector<bool>(ids.size(), false);
        }
    }

    std::vector<bool> success_flags;
    success_flags.reserve(ids.size());
    for (const Id& id : ids) {
        // Update the WAL ahead of time for durability and replay for primary and replicas.
        persistence_manager_map_[table_name]->AppendRemove(id);

        bool ok = store_map_[table_name]->Remove(id);
        if (ok) {
            // Indicate the table has been modified to trigger sync on all replicas.
            stats_manager_map_[table_name]->SetModifiedFlag(true);

            // Indicate the table has had a vector removed to trigger cleanup on HNSW index.
            stats_manager_map_[table_name]->SetRemovedFlag(true);

            stats_manager_map_[table_name]->Increment(StatsManager::StatType::DELETED);
            stats_manager_map_[table_name]->Increment(StatsManager::StatType::STALE);
            metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REMOVE, 1);
        }
        success_flags.push_back(ok);
    }

    if (metadata_.use_cache && cache_map_.contains(table_name) && stats_manager_map_[table_name]->GetModifiedFlag()) {
        cache_map_[table_name]->InvalidateAll();
    }

    return success_flags;
}

std::vector<std::vector<VectorStore::Data>> Engine::BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || (!store_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return {};
    }

    for (const auto& [query, k, search_param] : requests) {
        if (!input_validator_->ValidateSearch(table_name, query, k, search_param)) {
            return {};
        }
    }

    // Generate a unique hash as a key for the cache to store batch search request results.
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
        if (!cache_map_.contains(table_name)) {
            return {};
        }
        std::optional<Cache::CacheEntry> cache_entry = cache_map_[table_name]->Get(hash_key);
        if (cache_entry.has_value()) {
            // Return batched search results found directly in cache.
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

    // Use a thread pool to schedule search requests in parallel.
    std::vector<std::future<std::vector<VectorStore::Data>>> futures;
    for (const auto& [query, k, search_param] : requests) {
        futures.push_back(thread_pool_->EnqueueTask([this, table_name, query, k, search_param]() {
            // Measure search latency for metric calculations.
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
        // Save new batch results to the cache.
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
    if (shutdown_ || !stats_manager_map_.contains(table_name)) {
        return {};
    }

    if (!input_validator_->ValidateStats(table_name)) {
        return {};
    }

    return stats_manager_map_[table_name]->GetStats();
}

MetricsManager::Metrics Engine::GetMetrics(std::string table_name) {
    std::shared_lock lock(engine_mutex_);
    if (shutdown_ || !metrics_manager_map_.contains(table_name)) {
        return {};
    }

    if (!input_validator_->ValidateMetrics(table_name)) {
        return {};
    }

    return metrics_manager_map_[table_name]->GetMetrics();
}

bool Engine::CreateTable(std::string name, int vector_dimensionality, const HNSWIndex::HNSWIndexConfig& hnsw_index_config, const FlatIndex::FlatIndexConfig& flat_index_config, std::size_t cache_size) {
    std::unique_lock lock(engine_mutex_);
    if (!metadata_.primary || shutdown_) {
        return false;
    }

    if (!input_validator_->ValidateCreateTable(name, vector_dimensionality, hnsw_index_config, flat_index_config, cache_size)) {
        return false;
    }

    if (store_map_.contains(name) || persistence_manager_map_.contains(name) || stats_manager_map_.contains(name) || metrics_manager_map_.contains(name) || cache_map_.contains(name)) {
        return false;
    }

    std::string store_snapshot = name + "_" + kStoreSnapshotFilename;
    std::string index_snapshot = name + "_" + kIndexSnapshotFilename;
    std::string write_ahead_log = name + "_" + kWriteAheadLogFilename;
    std::string metadata = name + "_" + kMetadataFilename;

    std::unique_ptr<VectorIndex> index = nullptr;
    VectorStore::IndexType index_type = VectorStore::IndexType::FLAT;
    VectorPersistenceManager::StoredIndexType stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;

    // Check which index has been specified and create the relevant index.
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
        // Create a temporary persistence manager to update the WAL ahead of time for durability and replay for primary and replicas.
        auto temporary_persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, metadata, logger_.get());
        temporary_persistence_manager->AppendCreate(hnsw_index_config, flat_index_config, cache_size);
    }

    auto vector_store = std::make_unique<VectorStore>(index_type, std::move(index), vector_dimensionality);
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, metadata, logger_.get());
    auto metrics_manager = std::make_unique<MetricsManager>();
    auto stats_manager = std::make_unique<StatsManager>();
    auto lru_cache = std::make_unique<LRUCache>(cache_size);

    store_map_[name] = std::move(vector_store);
    persistence_manager_map_[name] = std::move(persistence_manager);
    stats_manager_map_[name] = std::move(stats_manager);
    metrics_manager_map_[name] = std::move(metrics_manager);
    cache_map_[name] = std::move(lru_cache);

    // Indicate table has been modified to trigger sync of table creation on replicas.
    stats_manager_map_[name]->SetModifiedFlag(true);

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
    std::string metadata = name + "_" + kMetadataFilename;

    std::unique_ptr<VectorIndex> index = nullptr;
    VectorStore::IndexType index_type = VectorStore::IndexType::FLAT;
    VectorPersistenceManager::StoredIndexType stored_index_type = VectorPersistenceManager::StoredIndexType::FLAT;

    // Check which index has been specified and create the relevant index.
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
    auto persistence_manager = std::make_unique<VectorPersistenceManager>(metadata_.name, stored_index_type, store_snapshot, index_snapshot, write_ahead_log, metadata, logger_.get());
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
    if (!metadata_.primary || shutdown_) {
        return false;
    }

    if (!input_validator_->ValidateDropTable(name)) {
        return {};
    }

    if (!store_map_.contains(name) || !persistence_manager_map_.contains(name) || !stats_manager_map_.contains(name) || !metrics_manager_map_.contains(name) || !cache_map_.contains(name)) {
        return false;
    }

    // Indicate table has been modified to trigger sync on all replicas.
    stats_manager_map_[name]->SetModifiedFlag(true);

    // Update the WAL ahead of time for durability and replay for primary and replicas.
    persistence_manager_map_[name]->AppendDrop();

    // Trigger sync directly before the table is destroyed on the primary.
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
    std::unique_lock replica_lock(replica_mutex_);
    std::unique_lock lock(engine_mutex_);
    if ((shutdown_ && !force) || (!store_map_.contains(table_name)|| !stats_manager_map_.contains(table_name) || !metrics_manager_map_.contains(table_name))) {
        return;
    }

    // Calculate the percentage of deleted vectors to check whether a reindex would be needed.
    StatsManager::Stats stats = stats_manager_map_[table_name]->GetStats();
    float ratio = 0;
    if (stats.vector_count + stats.stale_count - stats.deleted_count > 0) {
        ratio = static_cast<float>(stats.stale_count) / static_cast<float>(stats.vector_count + stats.stale_count - stats.deleted_count);
    }
    bool reindex = ratio > metadata_.reindex_threshold;

    // Measure the latency of the cleanup operation.
    auto start = std::chrono::steady_clock::now();
    store_map_[table_name]->Cleanup(reindex);
    auto end = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000;
    logger_->Info("cleanup completed in " + std::to_string(static_cast<float>(duration_ms)) + " ms", metadata_.name);

    metrics_manager_map_[table_name]->CalculateCleanupLatency(static_cast<float>(duration_ms));
    metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::CLEANUP, 1);

    // Reset the removed flag to avoid unnecessary cleanups.
    stats_manager_map_[table_name]->SetRemovedFlag(false);

    stats_manager_map_[table_name]->AdjustForDeletions();
    stats_manager_map_[table_name]->Reset(StatsManager::StatType::DELETED);

    if (reindex) {
        metrics_manager_map_[table_name]->Increment(MetricsManager::CountType::REINDEX, 1);
        stats_manager_map_[table_name]->Reset(StatsManager::StatType::STALE);
    }
}

bool Engine::ApplyWALEntry(const vector_db::WALEntry& entry) {
    if (entry.type() == vector_db::WALEntry::CREATE) {
        const auto& config = entry.create_config();
        const auto& store_config = config.store_config();
        auto& hnsw_config = config.hnsw_index_config();
        auto& flat_config = config.flat_index_config();
        const auto& cache_config = config.cache_config();

        bool ok = false;

        // Populate both configurations, where one should be constructed from Protobuf default values, indicating that it is not the right configuration. These will be validated when creating the table.
        HNSWIndex::HNSWIndexConfig hnsw_index_config = {hnsw_config.m(), hnsw_config.m0(), hnsw_config.ef_construction(), hnsw_config.ml(), (hnsw_config.distance_metric() == vector_db::WALEntry::CreateConfig::L2 ? VectorIndex::DistanceMetric::L2 : VectorIndex::DistanceMetric::Cosine), hnsw_config.vector_dimensionality()};
        FlatIndex::FlatIndexConfig flat_index_config = {flat_config.vector_dimensionality(), (flat_config.distance_metric() == vector_db::WALEntry::CreateConfig::L2 ? VectorIndex::DistanceMetric::L2 : VectorIndex::DistanceMetric::Cosine)};

        if (metadata_.primary) {
            // This case should currently never be used, but can be used in the future.
            ok = CreateTable(entry.table(), store_config.vector_dimensionality(), hnsw_index_config, flat_index_config, cache_config.cache_size());
        } else {
            std::unique_lock replica_lock(replica_mutex_);
            ok = CreateTableOnReplica(entry.table(), store_config.vector_dimensionality(), hnsw_index_config, flat_index_config, cache_config.cache_size());
        }
        return ok;
    }
    if (entry.type() == vector_db::WALEntry::DROP) {
        const auto& config = entry.drop_config();
        bool ok = false;
        if (metadata_.primary) {
            // This case should currently never be used, but can be used in the future.
            ok = DropTable(entry.table());
        } else {
            std::unique_lock replica_lock(replica_mutex_);
            ok = DropTableOnReplica(entry.table());
        }
        return ok;
    }

    std::unique_lock replica_lock(replica_mutex_);
    std::unique_lock lock(engine_mutex_);
    if (!store_map_.contains(entry.table()) || !stats_manager_map_.contains(entry.table())) {
        return false;
    }

    // Replay insert or remove operations against in-memory store directly.
    if (entry.type() == vector_db::WALEntry::INSERT) {
        const auto& config = entry.insert_config();
        Vector vector(config.vector().begin(), config.vector().end());
        bool ok = store_map_[entry.table()]->Insert(config.id(), vector, config.content());
        if (ok) {
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::VECTOR);
        }
        return ok;
    }
    if (entry.type() == vector_db::WALEntry::REMOVE) {
        const auto& config = entry.remove_config();
        bool ok = store_map_[entry.table()]->Remove(config.id());
        if (ok) {
            stats_manager_map_[entry.table()]->SetRemovedFlag(true);
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::DELETED);
            stats_manager_map_[entry.table()]->Increment(StatsManager::StatType::STALE);
        }
        return ok;
    }

    return false;
}

Logger* Engine::GetLogger() const {
    return logger_.get();
}

} // namespace vector_db_engine
