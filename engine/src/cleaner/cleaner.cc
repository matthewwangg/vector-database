#include "cleaner.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <shared_mutex>
#include <string>
#include <thread>

#include "engine.h"
#include "logger.h"
#include "stats_manager.h"
#include "vector_store.h"

namespace vector_db_engine {

Cleaner::Cleaner(std::string name, int cleanup_interval, std::atomic<bool>& shutdown, const std::function<void(const std::string&, bool)>& cleanup_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()>& get_store_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()>& get_stats_manager_map_callback,  Logger* logger)
    : name_(name),
      cleanup_interval_(cleanup_interval),
      shutdown_(shutdown),
      cleanup_callback_(cleanup_callback),
      get_store_map_callback_(get_store_map_callback),
      get_stats_manager_map_callback_(get_stats_manager_map_callback),
      logger_(logger)
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
        cleanup_cv_.wait_for(lock, std::chrono::milliseconds(cleanup_interval_));
        if (shutdown_) {
            break;
        }

        const auto& store_map = get_store_map_callback_();
        const auto& stats_manager_map = get_stats_manager_map_callback_();
        for (auto& [table, store] : store_map) {
            if (stats_manager_map.contains(table) && stats_manager_map.at(table)->GetRemovedFlag()) {
                auto start = std::chrono::steady_clock::now();
                cleanup_callback_(table, false);
                auto end = std::chrono::steady_clock::now();
                auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                logger_->Info("cleanup completed in " + std::to_string(duration_ms) + " ms", name_);
            }
        }
    }
}

} // namespace vector_db_engine
