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

Engine::Engine(std::unique_ptr<VectorIndex> index, int vector_dimensionality)
    : store_(std::make_unique<VectorStore>(std::move(index), vector_dimensionality)),
      persistence_manager_(std::make_unique<VectorPersistenceManager>(kStoreSnapshotFilename, kIndexSnapshotFilename)),
      shutdown_(false),
      removed_(false)
{
    std::unique_ptr<VectorStore> loaded_store = persistence_manager_->LoadSnapshot();
    if (loaded_store) {
        store_ = std::move(loaded_store);
    }
    cleanup_thread_ = std::thread(&Engine::BackgroundCleanupLoop, this);
}

Engine::~Engine() {
    shutdown_ = true;
    cleanup_cv_.notify_one();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    persistence_manager_->SaveSnapshot(*store_);
}

bool Engine::Insert(Id id, const Vector& vector, const std::string& content) {
    if (shutdown_) {
        return false;
    }

    return store_->Insert(id, vector, content);
}

bool Engine::Remove(Id id) {
    if (shutdown_) {
        return false;
    }

    removed_ = true;
    return store_->Remove(id);
}

std::vector<VectorStore::Data> Engine::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
    if (shutdown_) {
        return {};
    }

    return store_->Search(query, k, search_param);
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
            Cleanup();
            auto end = std::chrono::steady_clock::now();
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            std::cout << "cleanup completed in " << duration_ms << " ms\n";
        }
    }
}

void Engine::Cleanup() {
    store_->Cleanup();
    removed_ = false;
}

} // namespace vector_db_engine
