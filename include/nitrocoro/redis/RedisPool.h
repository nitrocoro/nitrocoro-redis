/**
 * @file RedisPool.h
 * @brief Redis connection pool for efficient connection reuse
 */
#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/redis/RedisConnection.h>

#include <functional>
#include <memory>

namespace nitrocoro::redis
{

struct PoolState;

class RedisPool
{
public:
    using Factory = std::function<Task<std::unique_ptr<RedisConnection>>()>;

    RedisPool(size_t maxSize, Factory factory, Scheduler * scheduler = Scheduler::current());
    ~RedisPool();
    RedisPool(const RedisPool &) = delete;
    RedisPool & operator=(const RedisPool &) = delete;

    [[nodiscard]] Task<std::unique_ptr<RedisConnection>> acquire();
    size_t idleCount() const;

private:
    std::shared_ptr<PoolState> state_;
    Factory factory_;
};

} // namespace nitrocoro::redis
