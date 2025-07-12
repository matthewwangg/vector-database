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
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads);
    ~ThreadPool();

    template<typename Function, typename... Arguments>
    auto EnqueueTask(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>>;

private:
    std::vector<std::thread> threads_;
    std::queue<std::function<void>> tasks_;

    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_;
};

template<typename Function, typename... Arguments>
auto ThreadPool::EnqueueTask(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>> {
    auto task = std::shared_ptr<std::invoke_result_t<Function, Arguments...>>(std::bind(std::forward<Function>(function), std::forward<Arguments>(arguments)...));
    std::future<std::invoke_result_t<Function, Arguments...>> result = task->get_future();
    {
        std::unique_lock lock(queue_mutex_);
        tasks_.emplace([task]() {
            (*task)();
        });
    }
    cv_.notify_one();
    return result;
}

#endif //VECTOR_DATABASE_THREAD_POOL_H
