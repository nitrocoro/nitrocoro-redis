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

static void detachConnection(const auto & weakState) noexcept
{
    auto state = weakState.lock();
    if (!state)
    {
        return;
    }

    state->scheduler->spawn([state]() -> Task<> {
        [[maybe_unused]] auto lock = co_await state->mutex.scoped_lock();
        --state->totalCount;
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

Task<PooledConnection> RedisPool::acquire()
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

    co_return PooledConnection{ std::move(conn), std::weak_ptr<PoolState>(state_) };
}

// PooledConnection implementation
PooledConnection::PooledConnection(std::unique_ptr<RedisConnection> conn, std::weak_ptr<RedisPool::PoolState> state)
    : conn_(std::move(conn)), state_(std::move(state))
{
}

PooledConnection::~PooledConnection()
{
    reset();
}

PooledConnection::PooledConnection(PooledConnection && other) noexcept
    : conn_(std::move(other.conn_)), state_(std::move(other.state_))
{
}

PooledConnection & PooledConnection::operator=(PooledConnection && other) noexcept
{
    if (this != &other)
    {
        reset();
        conn_ = std::move(other.conn_);
        state_ = std::move(other.state_);
    }
    return *this;
}

void PooledConnection::reset() noexcept
{
    if (conn_)
    {
        returnConnection(state_, conn_.release());
        state_.reset();
    }
}

std::unique_ptr<RedisConnection> PooledConnection::detach()
{
    if (conn_)
    {
        detachConnection(state_);
        state_.reset();
        return std::move(conn_);
    }
    return nullptr;
}

} // namespace nitrocoro::redis
