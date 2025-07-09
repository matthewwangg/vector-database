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

bool Engine::Insert(Id id, const Vector& vector, const std::string& content) {
    if (shutdown_) {
        return false;
    }

    persistence_manager_->AppendInsert(id, vector, content);

    bool ok = store_->Insert(id, vector, content);
    if (ok) {
        stats_.vector_count++;
    }
    return ok;
}

bool Engine::Remove(Id id) {
    if (shutdown_) {
        return false;
    }

    persistence_manager_->AppendRemove(id);

    bool ok = store_->Remove(id);
    if (ok) {
        removed_ = true;
        stats_.deleted_count++;
        stats_.stale_count++;
    }
    return ok;
}

std::vector<VectorStore::Data> Engine::Search(const Vector& query, std::size_t k, std::size_t search_param) const {
    if (shutdown_) {
        return {};
    }

    return store_->Search(query, k, search_param);
}

Engine::Stats Engine::GetStats() const {
    if (shutdown_) {
        return {};
    }

    return stats_;
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

void Engine::Cleanup(bool force) {
    if (shutdown_ && !force) {
        return;
    }

    float ratio = 0;
    if (stats_.vector_count + stats_.stale_count - stats_.deleted_count > 0) {
        ratio = static_cast<float>(stats_.stale_count) / static_cast<float>(stats_.vector_count + stats_.stale_count - stats_.deleted_count);
    }
    bool reindex = ratio > reindex_threshold_;

    store_->Cleanup(reindex);

    removed_ = false;
    stats_.vector_count = stats_.vector_count - stats_.deleted_count;
    stats_.deleted_count = 0;

    if (reindex) {
        stats_.stale_count = 0;
    }
}

} // namespace vector_db_engine
