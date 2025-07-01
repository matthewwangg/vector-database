#ifndef VECTOR_DATABASE_ENGINE_H
#define VECTOR_DATABASE_ENGINE_H

#include <atomic>
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
    explicit Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality);
    ~Engine();

    bool Insert(Id id, const Vector& vector, const std::string& content);
    bool Remove(Id id);
    std::vector<VectorStore::Data> Search(const Vector& query, std::size_t k, std::size_t search_param) const;

    void BackgroundCleanupLoop();
    void Cleanup();

private:
    std::unique_ptr<VectorStore> store_;
    std::unique_ptr<VectorPersistenceManager> persistence_manager_;

    std::thread cleanup_thread_;
    std::atomic<bool> shutdown_;
    std::atomic<bool> removed_;
};

}

#endif //VECTOR_DATABASE_ENGINE_H
