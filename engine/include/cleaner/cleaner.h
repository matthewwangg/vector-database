#ifndef VECTOR_DATABASE_CLEANER_H
#define VECTOR_DATABASE_CLEANER_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include "logger.h"
#include "stats_manager.h"
#include "vector_store.h"

namespace vector_db_engine {

class Engine;

// Cleaner is responsible for cleaning up the HNSW index of the soft removed nodes.
class Cleaner {
public:
    // Create the cleaner and start the background cleanup loop.
    explicit Cleaner(const std::string& name, int cleanup_interval, std::atomic<bool>& shutdown, const std::function<void(const std::string&, bool)>& cleanup_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()>& get_store_map_callback, const std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()>& get_stats_manager_map_callback, Logger* logger);

    // Ensure graceful shutdown by joining the background thread.
    ~Cleaner();

    // Starts the cleanup loop and is used on a background thread.
    void BackgroundCleanupLoop();

private:
    std::string name_;
    int cleanup_interval_;

    // Indicates to the background thread to shut down gracefully.
    std::atomic<bool>& shutdown_;

    // Callback that cleans up the vector index directly via the engine API.
    std::function<void(const std::string&, bool)> cleanup_callback_;

    std::function<const std::unordered_map<std::string, std::unique_ptr<StatsManager>>&()> get_stats_manager_map_callback_;
    std::function<const std::unordered_map<std::string, std::unique_ptr<VectorStore>>&()> get_store_map_callback_;

    std::thread cleanup_thread_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;

    Logger* logger_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_CLEANER_H
