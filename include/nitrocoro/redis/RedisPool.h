#pragma once

#include <nitrocoro/core/Future.h>
#include <nitrocoro/core/Mutex.h>
#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/redis/RedisConnection.h>

#include <functional>
#include <memory>
#include <queue>

namespace nitrocoro::redis
{

using nitrocoro::Mutex;
using nitrocoro::Promise;
using nitrocoro::Scheduler;
using nitrocoro::Task;

using PooledConnectionPtr = std::unique_ptr<RedisConnection, std::function<void(RedisConnection *)>>;

class RedisPool
{
public:
    using Factory = std::function<Task<std::unique_ptr<RedisConnection>>()>;

    RedisPool(size_t maxSize, Factory factory, Scheduler * scheduler = Scheduler::current())
        : factory_(std::move(factory)), scheduler_(scheduler), maxSize_(maxSize)
    {
    }
    RedisPool(const RedisPool &) = delete;
    RedisPool & operator=(const RedisPool &) = delete;

    [[nodiscard]] Task<PooledConnectionPtr> acquire();
    size_t idleCount() const { return idle_.size(); }

private:
    void returnConnection(RedisConnection * conn) noexcept;

    Factory factory_;
    Scheduler * scheduler_;
    size_t maxSize_;
    size_t totalCount_{ 0 };
    Mutex mutex_;
    std::queue<std::unique_ptr<RedisConnection>> idle_;
    std::queue<Promise<std::unique_ptr<RedisConnection>>> waiters_;
};

} // namespace nitrocoro::redis
