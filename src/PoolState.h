#pragma once

#include <nitrocoro/core/Future.h>
#include <nitrocoro/core/Mutex.h>
#include <nitrocoro/core/Scheduler.h>

#include <memory>
#include <queue>

namespace nitrocoro::redis
{

class RedisConnectionImpl;

struct PoolState
{
    Scheduler * scheduler;
    size_t maxSize;
    size_t totalCount = 0;
    Mutex mutex;
    std::queue<std::unique_ptr<RedisConnectionImpl>> idle;
    std::queue<Promise<std::unique_ptr<RedisConnectionImpl>>> waiters;

    static void returnConnection(const std::weak_ptr<PoolState> & weakState,
                                 std::unique_ptr<RedisConnectionImpl> conn) noexcept;
    static void detachConnection(const std::weak_ptr<PoolState> & weakState) noexcept;
};

} // namespace nitrocoro::redis
