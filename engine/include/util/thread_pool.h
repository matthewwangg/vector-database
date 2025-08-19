#ifndef VECTOR_DATABASE_THREAD_POOL_H
#define VECTOR_DATABASE_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace vector_db_engine {

// ThreadPool is responsible for thread-safe concurrent task execution, specifically for batch search operations.
class ThreadPool {
public:
    // Create the thread pool by spawning num_threads threads immediately.
    explicit ThreadPool(std::size_t num_threads);

    // Ensures graceful shutdown by notifying all threads and joining all threads.
    ~ThreadPool();

    // Enqueues the functions to the task queue, regardless of function type or arguments. Returns a future of the task result.
    template<typename Function, typename... Arguments>
    auto EnqueueTask(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>>;

private:
    std::vector<std::thread> threads_;

    // FIFO queue that holds all the tasks.
    std::queue<std::function<void()>> tasks_;

    // Controls access to the task queue by each thread in the thread pool.
    std::mutex queue_mutex_;

    std::condition_variable cv_;

    // Indicates to all threads to stop gracefully.
    std::atomic<bool> stop_;
};


template<typename Function, typename... Arguments>
auto ThreadPool::EnqueueTask(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>> {
    if (stop_) {
        return std::future<std::invoke_result_t<Function, Arguments...>>();
    }

    // Wrap the function in a packaged task to be run later.
    auto task = std::make_shared<std::packaged_task<std::invoke_result_t<Function, Arguments...>()>>(std::bind(std::forward<Function>(function), std::forward<Arguments>(arguments)...));

    std::future<std::invoke_result_t<Function, Arguments...>> result = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        // Place a void function that wraps the task in the queue
        tasks_.emplace([task]() {
            (*task)();
        });
    }

    // Notify a worker that a task is available.
    cv_.notify_one();

    return result;
}

} // namespace vector_db_engine

#endif //VECTOR_DATABASE_THREAD_POOL_H
