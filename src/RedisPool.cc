#include "nitrocoro/redis/RedisPool.h"

namespace nitrocoro::redis
{

struct RedisPool::PoolState
{
    Scheduler * scheduler;
    size_t maxSize;
    size_t totalCount = 0;
    Mutex mutex;
    std::queue<std::unique_ptr<RedisConnection>> idle;
    std::queue<Promise<std::unique_ptr<RedisConnection>>> waiters;
};

static void returnConnection(const auto & weakState, RedisConnection * conn) noexcept
{
    auto state = weakState.lock();
    if (!state)
    {
        // Pool已销毁，直接删除连接
        delete conn;
        return;
    }

    std::unique_ptr<RedisConnection> connPtr(conn);
    state->scheduler->spawn([state, connPtr = std::move(connPtr)]() mutable -> Task<> {
        [[maybe_unused]] auto lock = co_await state->mutex.scoped_lock();
        if (!state->waiters.empty())
        {
            state->waiters.front().set_value(std::move(connPtr));
            state->waiters.pop();
        }
        else
        {
            state->idle.push(std::move(connPtr));
        }
    });
}

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

Task<PooledConnectionPtr> RedisPool::acquire()
{
    std::unique_ptr<RedisConnection> conn;
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
            Promise<std::unique_ptr<RedisConnection>> promise(state_->scheduler);
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
            conn = co_await factory_();
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

    co_return PooledConnectionPtr{
        conn.release(),
        [weakState = std::weak_ptr<PoolState>(state_)](RedisConnection * ptr) {
            returnConnection(weakState, ptr);
        }
    };
}

} // namespace nitrocoro::redis
