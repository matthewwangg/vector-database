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
    cv_.notify_all();

    for (auto& thread : threads_) {
        thread.join();
    }
}

}
