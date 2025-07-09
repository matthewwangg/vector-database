#ifndef VECTOR_DATABASE_ENGINE_H
#define VECTOR_DATABASE_ENGINE_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "persistence_manager.h"
#include "vector_store.h"

namespace vector_db_engine {

class Engine {
public:
    struct Stats {
        uint64_t vector_count = 0;
        uint64_t deleted_count = 0;

        uint64_t vector_count_at_last_reindex = 0;
        uint64_t stale_count = 0;
    };

    explicit Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality, float reindex_threshold);
    ~Engine();

    bool Insert(Id id, const Vector& vector, const std::string& content);
    bool Remove(Id id);
    std::vector<VectorStore::Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

    Stats GetStats() const;

    void BackgroundCleanupLoop();
    void Cleanup(bool force);

private:
    std::unique_ptr<VectorStore> store_;
    std::unique_ptr<VectorPersistenceManager> persistence_manager_;

    Stats stats_;

    float reindex_threshold_;

    std::thread cleanup_thread_;
    std::atomic<bool> shutdown_;
    std::atomic<bool> removed_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;
};

}

#endif //VECTOR_DATABASE_ENGINE_H
