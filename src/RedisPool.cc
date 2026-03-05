/**
 * @file RedisPool.cc
 * @brief Implementation of Redis connection pool
 */
#include <nitrocoro/redis/RedisPool.h>

#include "PoolState.h"
#include "PooledConnection.h"
#include "RedisConnectionImpl.h"

namespace nitrocoro::redis
{

RedisPool::RedisPool(size_t maxSize, Factory factory, Scheduler * scheduler)
    : state_(std::make_shared<PoolState>(scheduler, maxSize))
    , factory_(std::move(factory))
{
}

RedisPool::~RedisPool() = default;

size_t RedisPool::idleCount() const
{
    return state_->idle.size();
}

Task<std::unique_ptr<RedisConnection>> RedisPool::acquire()
{
    std::unique_ptr<RedisConnectionImpl> conn;
    {
        [[maybe_unused]] auto lock = co_await state_->mutex.scoped_lock();
        if (!state_->idle.empty())
        {
            conn = std::move(state_->idle.front());
            state_->idle.pop();
        }
        else if (state_->totalCount < state_->maxSize)
        {
            ++state_->totalCount;
        }
        else
        {
            Promise<std::unique_ptr<RedisConnectionImpl>> promise(state_->scheduler);
            auto future = promise.get_future();
            state_->waiters.push(std::move(promise));
            lock.unlock();
            conn = co_await future.get();
        }
    }

    if (!conn)
    {
        std::exception_ptr err;
        try
        {
            auto baseConn = co_await factory_();
            conn = std::unique_ptr<RedisConnectionImpl>(static_cast<RedisConnectionImpl *>(baseConn.release()));
        }
        catch (...)
        {
            err = std::current_exception();
        }

        if (err)
        {
            [[maybe_unused]] auto lock = co_await state_->mutex.scoped_lock();
            --state_->totalCount;
            std::rethrow_exception(err);
        }
    }

    co_return std::make_unique<PooledConnection>(std::move(conn), std::weak_ptr<PoolState>(state_));
}

} // namespace nitrocoro::redis
