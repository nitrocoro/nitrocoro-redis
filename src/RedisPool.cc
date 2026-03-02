#include "nitrocoro/redis/RedisPool.h"

namespace nitrocoro::redis
{

Task<PooledConnectionPtr> RedisPool::acquire()
{
    std::unique_ptr<RedisConnection> conn;
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
            Promise<std::unique_ptr<RedisConnection>> promise(scheduler_);
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

    co_return PooledConnectionPtr{
        conn.release(),
        [this](RedisConnection * ptr) {
            returnConnection(ptr);
        }
    };
}

void RedisPool::returnConnection(RedisConnection * conn) noexcept
{
    std::unique_ptr<RedisConnection> connPtr(conn);
    scheduler_->spawn([this, connPtr = std::move(connPtr)]() mutable -> Task<> {
        [[maybe_unused]] auto lock = co_await mutex_.scoped_lock();
        if (!waiters_.empty())
        {
            waiters_.front().set_value(std::move(connPtr));
            waiters_.pop();
        }
        else
        {
            idle_.push(std::move(connPtr));
        }
    });
}

} // namespace nitrocoro::redis
