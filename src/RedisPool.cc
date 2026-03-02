#include "nitrocoro/redis/RedisPool.h"

#include <nitrocoro/utils/Debug.h>

namespace nitrocoro::redis
{

PooledConnection::PooledConnection(std::shared_ptr<RedisConnection> conn,
                                   std::function<void(std::shared_ptr<RedisConnection>)> returnFn)
    : conn_(std::move(conn)), returnFn_(std::move(returnFn))
{
}

PooledConnection::~PooledConnection() noexcept
{
    if (conn_)
    {
        try
        {
            returnFn_(std::move(conn_));
        }
        catch (...)
        {
        }
    }
}

Task<PooledConnection> RedisPool::acquire()
{
    std::shared_ptr<RedisConnection> conn;
    {
        [[maybe_unused]] auto lock = co_await mutex_.scoped_lock();
        if (!idle_.empty())
        {
            conn = std::move(idle_.front());
            idle_.pop();
        }
        else if (totalCount_ < maxSize_)
        {
            ++totalCount_;
        }
        else
        {
            Promise<std::shared_ptr<RedisConnection>> promise(scheduler_);
            auto future = promise.get_future();
            waiters_.push(std::move(promise));
            lock.unlock();
            conn = co_await future.get();
        }
    }

    if (!conn)
    {
        std::exception_ptr err;
        try
        {
            conn = co_await factory_();
        }
        catch (...)
        {
            err = std::current_exception();
        }

        if (err)
        {
            [[maybe_unused]] auto lock = co_await mutex_.scoped_lock();
            --totalCount_;
            std::rethrow_exception(err);
        }
    }

    co_return PooledConnection(std::move(conn), [this](std::shared_ptr<RedisConnection> c) {
        returnConnection(std::move(c));
    });
}

void RedisPool::returnConnection(std::shared_ptr<RedisConnection> conn) noexcept
{
    scheduler_->spawn([this, conn = std::move(conn)]() mutable -> Task<> {
        [[maybe_unused]] auto lock = co_await mutex_.scoped_lock();
        if (!waiters_.empty())
        {
            waiters_.front().set_value(std::move(conn));
            waiters_.pop();
        }
        else
        {
            idle_.push(std::move(conn));
        }
    });
}

} // namespace nitrocoro::redis
