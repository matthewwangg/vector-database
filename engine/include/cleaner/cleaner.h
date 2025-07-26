#ifndef VECTOR_DATABASE_CLEANER_H
#define VECTOR_DATABASE_CLEANER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "logger.h"

namespace vector_db_engine {

class Engine;

class Cleaner {
public:
    explicit Cleaner(Engine* engine, std::string name, std::atomic<bool>& shutdown);
    ~Cleaner();

    void BackgroundCleanupLoop();

private:
    Engine* engine_;

    std::string name_;
    std::atomic<bool>& shutdown_;

    std::thread cleanup_thread_;
    std::atomic<bool> removed_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;
};

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_CLEANER_H
