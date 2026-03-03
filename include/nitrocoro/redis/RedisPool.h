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

class PooledConnection;

class RedisPool
{
public:
    struct PoolState;
    using Factory = std::function<Task<RedisConnection>()>;

    RedisPool(size_t maxSize, Factory factory, Scheduler * scheduler = Scheduler::current());
    ~RedisPool();
    RedisPool(const RedisPool &) = delete;
    RedisPool & operator=(const RedisPool &) = delete;

    [[nodiscard]] Task<PooledConnection> acquire();
    size_t idleCount() const;

private:
    std::shared_ptr<PoolState> state_;
    Factory factory_;
};

class PooledConnection
{
public:
    PooledConnection(std::unique_ptr<RedisConnection> conn, std::weak_ptr<RedisPool::PoolState> state);
    PooledConnection();
    ~PooledConnection();

    PooledConnection(const PooledConnection &) = delete;
    PooledConnection & operator=(const PooledConnection &) = delete;
    PooledConnection(PooledConnection && other) noexcept;
    PooledConnection & operator=(PooledConnection && other) noexcept;

    RedisConnection * operator->() const noexcept { return conn_.get(); }
    RedisConnection & operator*() const noexcept { return *conn_; }

    explicit operator bool() const noexcept { return static_cast<bool>(conn_); }

    /**
     * @brief Return connection to pool and reset to empty state
     *
     * After calling reset(), this PooledConnection becomes empty and the connection
     * is returned to the pool for reuse by other acquire() calls.
     */
    void reset() noexcept;

    /**
     * @brief Detach connection from pool and transfer ownership to caller
     *
     * The connection will no longer be managed by the pool and will not be
     * automatically returned. The caller is responsible for the connection's lifetime.
     * This decreases the pool's total connection count.
     *
     * @return RedisConnection value, or throws if this object is empty
     */
    RedisConnection detach();

private:
    std::unique_ptr<RedisConnection> conn_;
    std::weak_ptr<RedisPool::PoolState> state_;
};

} // namespace nitrocoro::redis
