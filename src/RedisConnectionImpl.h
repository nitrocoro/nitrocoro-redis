/**
 * @file RedisConnectionImpl.h
 * @brief Internal implementation of Redis connection
 */
#pragma once

#include <nitrocoro/core/Future.h>
#include <nitrocoro/io/Channel.h>
#include <nitrocoro/redis/RedisConnection.h>

#include <hiredis/async.h>

#include <memory>

namespace nitrocoro::redis
{

struct RedisAsyncDeleter
{
    void operator()(redisAsyncContext * ctx) const
    {
        if (ctx)
            redisAsyncFree(ctx);
    }
};

using RedisAsyncContextPtr = std::unique_ptr<redisAsyncContext, RedisAsyncDeleter>;

struct ConnectionContext;

class RedisConnectionImpl : public RedisConnection
{
public:
    explicit RedisConnectionImpl(std::shared_ptr<ConnectionContext> ctx);
    ~RedisConnectionImpl() override;

    const std::string & host() const override;
    uint16_t port() const override;
    bool isAlive() const override;

protected:
    Task<RedisResult> executeFormatted(const char * cmd, int len) override;

private:
    friend class PooledConnection;
    std::shared_ptr<ConnectionContext> ctx_;
};

} // namespace nitrocoro::redis
