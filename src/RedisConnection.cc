#include "nitrocoro/redis/RedisConnection.h"

#include <nitrocoro/core/Future.h>
#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Types.h>
#include <nitrocoro/io/IoChannel.h>
#include <nitrocoro/utils/Debug.h>

#include <hiredis/async.h>

#include <stdexcept>

namespace nitrocoro::redis
{

using nitrocoro::Promise;
using nitrocoro::Scheduler;
using nitrocoro::Task;
using nitrocoro::TriggerMode;
using nitrocoro::io::IoChannel;

struct RedisAsyncDeleter
{
    void operator()(redisAsyncContext * ctx) const
    {
        if (ctx)
            redisAsyncFree(ctx);
    }
};

using RedisAsyncContextPtr = std::unique_ptr<redisAsyncContext, RedisAsyncDeleter>;

struct RedisConnection::ConnectionContext
{
    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<IoChannel> channel;
    std::string host;
    uint16_t port;
    Scheduler * scheduler;
    bool running{ true };
    bool disconnecting{ false };
    std::unique_ptr<Promise<>> connectPromise;
    std::unique_ptr<Promise<>> disconnectPromise;
};

Task<std::unique_ptr<RedisConnection>> RedisConnection::connect(std::string host, uint16_t port, Scheduler * scheduler)
{
    NITRO_TRACE("[Redis] Connecting to %s:%hu\n", host.c_str(), port);

    // Step 1: Create async connection
    RedisAsyncContextPtr redisCtxPtr(redisAsyncConnect(host.c_str(), port));
    auto * redisCtx = redisCtxPtr.get();
    if (!redisCtx || redisCtx->err)
    {
        std::string err = redisCtx ? redisCtx->errstr : "allocation failed";
        NITRO_ERROR("[Redis] redisAsyncConnect failed: %s\n", err.c_str());
        throw std::runtime_error("Redis connection failed: " + err);
    }
    NITRO_TRACE("[Redis] redisAsyncConnect created, fd=%d\n", redisCtx->c.fd);

    // Step 2: Switch to scheduler thread
    co_await scheduler->switch_to();

    // Step 3: Create IO context with all resources
    auto channel = std::make_unique<IoChannel>(redisCtx->c.fd, TriggerMode::LevelTriggered, scheduler);
    auto connCtx = std::make_shared<ConnectionContext>(ConnectionContext{
        .redisCtx = std::move(redisCtxPtr),
        .channel = std::move(channel),
        .host = std::move(host),
        .port = port,
        .scheduler = scheduler,
        .connectPromise = std::make_unique<Promise<>>(scheduler) });

    // Step 4: Setup hiredis event hooks
    redisCtx->ev.data = connCtx.get();
    redisCtx->ev.addRead = [](void * privdata) {
        auto * ctx = static_cast<ConnectionContext *>(privdata);
        ctx->channel->enableReading();
    };
    redisCtx->ev.delRead = [](void * privdata) {
        auto * ctx = static_cast<ConnectionContext *>(privdata);
        ctx->channel->disableReading();
    };
    redisCtx->ev.addWrite = [](void * privdata) {
        auto * ctx = static_cast<ConnectionContext *>(privdata);
        ctx->channel->enableWriting();
    };
    redisCtx->ev.delWrite = [](void * privdata) {
        auto * ctx = static_cast<ConnectionContext *>(privdata);
        ctx->channel->disableWriting();
    };

    // Step 5: Register callbacks
    int ret = redisAsyncSetConnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<ConnectionContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)\n", status, status == REDIS_OK ? "OK" : "ERROR");
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s\n", c->errstr);
            // hiredis says that asyncCtx will auto free by hiredis on connect failure
            void * _ = ctx->redisCtx.release();
            ctx->connectPromise->set_exception(std::make_exception_ptr(std::runtime_error(c->errstr)));
        }
        else
        {
            NITRO_TRACE("[Redis] Connection successful\n");
            ctx->connectPromise->set_value();
        }
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set connect callback\n");
        throw std::runtime_error("Failed to set connect callback");
    }

    ret = redisAsyncSetDisconnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<ConnectionContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Disconnect callback: status=%d\n", status);
        ctx->running = false;

        if (ctx->channel)
        {
            ctx->channel->disableAll();
            ctx->channel->cancelAll();
        }

        // hiredis says that asyncCtx will auto free by hiredis on disconnect
        void * _ = ctx->redisCtx.release();

        if (ctx->disconnectPromise)
            ctx->disconnectPromise->set_value();
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set disconnect callback\n");
        throw std::runtime_error("Failed to set disconnect callback");
    }

    // Step 6: Start read/write coroutines
    scheduler->spawn([connCtx]() -> Task<> {
        NITRO_TRACE("[Redis] Read coroutine started\n");
        co_await connCtx->channel->performRead([&](int, IoChannel *) -> IoChannel::IoStatus {
            if (!connCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleRead(connCtx->redisCtx.get());
            if (!connCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            return IoChannel::IoStatus::NeedRead;
        });
        NITRO_TRACE("[Redis] Read coroutine finished\n");
    });

    scheduler->spawn([connCtx]() -> Task<> {
        NITRO_TRACE("[Redis] Write coroutine started\n");
        co_await connCtx->channel->performWrite([&](int, IoChannel *) -> IoChannel::IoStatus {
            if (!connCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleWrite(connCtx->redisCtx.get());
            if (!connCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            return IoChannel::IoStatus::NeedWrite;
        });
        NITRO_TRACE("[Redis] Write coroutine finished\n");
    });

    // Step 7: Wait for connection to complete
    NITRO_TRACE("[Redis] Waiting for connection to complete...\n");
    co_await connCtx->connectPromise->get_future().get();
    NITRO_TRACE("[Redis] Connection completed successfully\n");
    co_return std::unique_ptr<RedisConnection>(new RedisConnection(std::move(connCtx)));
}

RedisConnection::RedisConnection(std::shared_ptr<ConnectionContext> ctx)
    : ctx_(std::move(ctx))
{
}

RedisConnection::~RedisConnection()
{
    // Schedule cleanup on scheduler thread
    ctx_->scheduler->dispatch([ctx = std::move(ctx_)]() {
        if (!ctx->running)
            return;
        ctx->running = false;

        if (ctx->channel)
        {
            ctx->channel->disableAll();
            ctx->channel->cancelAll();
        }

        if (ctx->redisCtx && !ctx->disconnecting)
        {
            ctx->disconnecting = true;
            redisAsyncDisconnect(ctx->redisCtx.get());
        }
    });
}

const std::string & RedisConnection::host() const
{
    return ctx_->host;
}

uint16_t RedisConnection::port() const
{
    return ctx_->port;
}

Task<Result> RedisConnection::executeFormatted(const char * cmd, int len)
{
    if (ctx_->disconnecting)
        throw std::runtime_error("Connection is disconnecting");

    // Create callback context
    struct CallbackContext
    {
        Promise<Result> promise;
    };

    auto cbCtxPtr = std::make_unique<CallbackContext>(CallbackContext{ Promise<Result>(ctx_->scheduler) });
    auto future = cbCtxPtr->promise.get_future();

    // Send command
    int ret = redisAsyncFormattedCommand(
        ctx_->redisCtx.get(),
        [](redisAsyncContext *, void * reply, void * privdata) {
            auto * cbCtx = static_cast<CallbackContext *>(privdata);
            auto * r = static_cast<redisReply *>(reply);
            if (!r)
            {
                cbCtx->promise.set_exception(std::make_exception_ptr(std::runtime_error("No reply")));
                return;
            }
            try
            {
                cbCtx->promise.set_value(Result::fromRaw(r));
            }
            catch (...)
            {
                cbCtx->promise.set_exception(std::current_exception());
            }
        },
        cbCtxPtr.get(),
        cmd,
        len);

    if (ret != REDIS_OK)
        throw std::runtime_error("Failed to send command");

    co_return co_await future.get();
}

Task<> RedisConnection::disconnect()
{
    co_await ctx_->scheduler->switch_to();

    if (ctx_->disconnecting)
        co_return;
    ctx_->disconnecting = true;

    ctx_->disconnectPromise = std::make_unique<Promise<>>(ctx_->scheduler);
    auto future = ctx_->disconnectPromise->get_future();

    NITRO_TRACE("[Redis] Disconnecting...\n");
    redisAsyncDisconnect(ctx_->redisCtx.get());

    co_await future.get();
    NITRO_TRACE("[Redis] Disconnected\n");
}

std::pair<std::unique_ptr<char, void (*)(char *)>, int> RedisConnection::formatCommand(const char * format, ...)
{
    va_list ap;
    va_start(ap, format);

    char * rawCmd = nullptr;
    int len = redisvFormatCommand(&rawCmd, format, ap);

    va_end(ap);

    if (len == -2)
        throw std::runtime_error("Invalid format string");
    if (len < 0)
        throw std::runtime_error("Failed to format command");

    return { std::unique_ptr<char, void (*)(char *)>(rawCmd, redisFreeCommand), len };
}

} // namespace nitrocoro::redis
