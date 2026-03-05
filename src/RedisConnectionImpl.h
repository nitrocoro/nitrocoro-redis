#pragma once

#include <nitrocoro/core/Future.h>
#include <nitrocoro/io/IoChannel.h>
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

struct ConnectionContext
{
    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<io::IoChannel> channel;
    std::string host;
    uint16_t port;
    Scheduler * scheduler;
    bool running{ true };
    bool disconnecting{ false };
    std::unique_ptr<Promise<>> connectPromise;
    std::unique_ptr<Promise<>> disconnectPromise;
};

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
