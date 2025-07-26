#include "cleaner.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <shared_mutex>
#include <string>
#include <thread>

#include "engine.h"

namespace vector_db_engine {

constexpr int kCleanupInterval = 60;

Cleaner::Cleaner(Engine* engine, std::string name, std::atomic<bool>& shutdown)
    : engine_(engine),
      name_(name),
      shutdown_(shutdown)
{
    cleanup_thread_ = std::thread(&Cleaner::BackgroundCleanupLoop, this);
}

Cleaner::~Cleaner() {
    cleanup_cv_.notify_one();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

void Cleaner::BackgroundCleanupLoop() {
    std::unique_lock<std::mutex> lock(cleanup_mutex_);
    while (!shutdown_) {
        cleanup_cv_.wait_for(lock, std::chrono::seconds(kCleanupInterval));

        if (shutdown_) {
            break;
        }

        for (auto& [table, store] : engine_->GetStoreMap()) {
            if (engine_->GetStatsManagerMap().contains(table) && engine_->GetStatsManagerMap().at(table)->GetRemovedFlag()) {
                auto start = std::chrono::steady_clock::now();
                engine_->Cleanup(table, false);
                auto end = std::chrono::steady_clock::now();
                auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                engine_->GetLogger()->Info("cleanup completed in " + std::to_string(duration_ms) + " ms", name_);
            }
        }
    }
}

} // namespace vector_db_engine
