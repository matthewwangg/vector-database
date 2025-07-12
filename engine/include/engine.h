#ifndef VECTOR_DATABASE_ENGINE_H
#define VECTOR_DATABASE_ENGINE_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "persistence_manager.h"
#include "vector_store.h"

namespace vector_db_engine {

class Engine {
public:
    struct Stats {
        uint64_t vector_count = 0;
        uint64_t deleted_count = 0;
        uint64_t stale_count = 0;
    };

    struct Metrics {
        uint64_t insert_count = 0;
        uint64_t remove_count = 0;
        uint64_t search_count = 0;
        uint64_t cleanup_count = 0;
        uint64_t reindex_count = 0;

        uint64_t average_search_latency_ms = 0;
    };

    explicit Engine(float reindex_threshold);
    ~Engine();

    bool Insert(std::string table_name, Id id, const Vector& vector, const std::string& content);
    bool Remove(std::string table_name, Id id);
    std::vector<VectorStore::Data> Search(std::string table_name, const Vector& query, std::size_t k, std::size_t search_param);

    std::vector<bool> BatchInsert(std::string table_name, const std::vector<std::tuple<Id, Vector, std::string>> vectors);
    std::vector<bool> BatchRemove(std::string table_name, std::vector<Id> ids);
    std::vector<std::vector<VectorStore::Data>> BatchSearch(std::string table_name, const std::vector<std::tuple<Vector, std::size_t, std::size_t>>& requests);

    Stats GetStats(std::string table_name);
    Metrics GetMetrics(std::string table_name);

    bool CreateTable(std::string name);
    bool DropTable(std::string name);

    void BackgroundCleanupLoop();
    void Cleanup(const std::string& table_name, bool force);

private:
    std::unique_ptr<VectorStore> store_;
    std::unique_ptr<VectorPersistenceManager> persistence_manager_;
    Stats stats_;
    Metrics metrics_;

    std::unordered_map<std::string, std::unique_ptr<VectorStore>> store_map_;
    std::unordered_map<std::string, std::unique_ptr<VectorPersistenceManager>> persistence_manager_map_;
    std::unordered_map<std::string, Stats> stats_map_;
    std::unordered_map<std::string, Metrics> metrics_map_;
    std::unordered_map<std::string, std::atomic<bool>> removed_flag_map_;

    mutable std::shared_mutex engine_mutex_;

    float reindex_threshold_;

    std::thread cleanup_thread_;
    std::atomic<bool> shutdown_;
    std::atomic<bool> removed_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_ENGINE_H
