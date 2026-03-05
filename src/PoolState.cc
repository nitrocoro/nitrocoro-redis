#include "PoolState.h"
#include "RedisConnectionImpl.h"

namespace nitrocoro::redis
{

void PoolState::returnConnection(const std::weak_ptr<PoolState> & weakState,
                                 std::unique_ptr<RedisConnectionImpl> conn) noexcept
{
    auto state = weakState.lock();
    if (!state)
        return;

    state->scheduler->spawn([state, conn = std::move(conn)]() mutable -> Task<> {
        [[maybe_unused]] auto lock = co_await state->mutex.scoped_lock();
        if (!state->waiters.empty())
        {
            state->waiters.front().set_value(std::move(conn));
            state->waiters.pop();
        }
        else
        {
            state->idle.push(std::move(conn));
        }
    });
}

void PoolState::detachConnection(const std::weak_ptr<PoolState> & weakState) noexcept
{
    auto state = weakState.lock();
    if (!state)
        return;

    state->scheduler->spawn([state]() -> Task<> {
        [[maybe_unused]] auto lock = co_await state->mutex.scoped_lock();
        --state->totalCount;
    });
}

} // namespace nitrocoro::redis
