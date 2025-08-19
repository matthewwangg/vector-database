#include "thread_pool.h"

#include <cstddef>
#include <functional>
#include <utility>

namespace vector_db_engine {

ThreadPool::ThreadPool(std::size_t num_threads)
    : stop_(false)
{
    for (std::size_t i = 0; i < num_threads; ++i) {
        threads_.emplace_back([this]() {
            while (true) {
                std::function<void()> task;

                {
                    std::unique_lock lock(queue_mutex_);
                    cv_.wait(lock, [this]() {
                        return stop_ || !tasks_.empty();
                    });

                    // Only stop this thread once the stop flag has been set and there are no remaining tasks.
                    if (stop_ && tasks_.empty()) {
                        break;
                    }

                    task = std::move(tasks_.front());
                    tasks_.pop();
                }

                task();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock lock(queue_mutex_);
        stop_ = true;
    }

    // Notify all threads to clear out any tasks left in the queue and to stop.
    cv_.notify_all();

    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

}
