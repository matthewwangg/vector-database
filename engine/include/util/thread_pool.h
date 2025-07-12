#ifndef VECTOR_DATABASE_THREAD_POOL_H
#define VECTOR_DATABASE_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <queue>
#include <mutex>
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads);
    ~ThreadPool();

    template<typename Function, typename... Arguments>
    auto Enqueue(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>>;

private:
    std::vector<std::thread> threads_;
    std::queue<std::function<void>> tasks_;

    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_;
};

template<typename Function, typename... Arguments>
auto Enqueue(Function&& function, Arguments&&... arguments) -> std::future<std::invoke_result_t<Function, Arguments...>> {

}

#endif //VECTOR_DATABASE_THREAD_POOL_H
