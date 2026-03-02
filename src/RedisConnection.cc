#include "nitrocoro/redis/RedisConnection.h"

#include <nitrocoro/core/Future.h>
#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Types.h>
#include <nitrocoro/io/IoChannel.h>
#include <nitrocoro/utils/Debug.h>

#include <hiredis/async.h>

#include <cstring>
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

struct RedisConnection::IoContext
{
    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<IoChannel> channel;
    bool running{ true };
    bool disconnecting{ false };
    std::unique_ptr<Promise<>> connectPromise;
    std::unique_ptr<Promise<>> disconnectPromise;
};

RedisConnection::RedisConnection(std::string host, int port, Scheduler * scheduler)
    : host_(std::move(host)), port_(port), scheduler_(scheduler)
{
}

RedisConnection::~RedisConnection()
{
    if (!ioCtx_)
        return;

    // Schedule cleanup on scheduler thread
    scheduler_->dispatch([ioCtx = std::move(ioCtx_)]() {
        if (!ioCtx->running)
            return;
        ioCtx->running = false;

        if (ioCtx->channel)
        {
            ioCtx->channel->disableAll();
            ioCtx->channel->cancelAll();
        }

        if (ioCtx->redisCtx && !ioCtx->disconnecting)
        {
            ioCtx->disconnecting = true;
            redisAsyncDisconnect(ioCtx->redisCtx.get());
        }
    });
}

Task<> RedisConnection::connect()
{
    NITRO_TRACE("[Redis] Connecting to %s:%d\n", host_.c_str(), port_);

    // Step 1: Create async connection
    RedisAsyncContextPtr redisCtxPtr(redisAsyncConnect(host_.c_str(), port_));
    auto * redisCtx = redisCtxPtr.get();
    if (!redisCtx || redisCtx->err)
    {
        std::string err = redisCtx ? redisCtx->errstr : "allocation failed";
        NITRO_ERROR("[Redis] redisAsyncConnect failed: %s\n", err.c_str());
        throw std::runtime_error("Redis connection failed: " + err);
    }
    NITRO_TRACE("[Redis] redisAsyncConnect created, fd=%d\n", redisCtx->c.fd);

    // Step 2: Switch to scheduler thread
    co_await scheduler_->switch_to();

    // Step 3: Create IO context with all resources
    ioCtx_ = std::make_shared<IoContext>();
    ioCtx_->redisCtx = std::move(redisCtxPtr);
    ioCtx_->channel = std::make_unique<IoChannel>(ioCtx_->redisCtx->c.fd, TriggerMode::LevelTriggered, scheduler_);
    ioCtx_->connectPromise = std::make_unique<Promise<>>(scheduler_);

    // Step 4: Setup hiredis event hooks
    redisCtx->ev.data = ioCtx_.get();
    redisCtx->ev.addRead = [](void * privdata) {
        auto * ctx = static_cast<IoContext *>(privdata);
        ctx->channel->enableReading();
    };
    redisCtx->ev.delRead = [](void * privdata) {
        auto * ctx = static_cast<IoContext *>(privdata);
        ctx->channel->disableReading();
    };
    redisCtx->ev.addWrite = [](void * privdata) {
        auto * ctx = static_cast<IoContext *>(privdata);
        ctx->channel->enableWriting();
    };
    redisCtx->ev.delWrite = [](void * privdata) {
        auto * ctx = static_cast<IoContext *>(privdata);
        ctx->channel->disableWriting();
    };

    // Step 5: Register callbacks
    int ret = redisAsyncSetConnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<IoContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)\n", status, status == REDIS_OK ? "OK" : "ERROR");
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s\n", c->errstr);
            // it is said that asyncCtx will auto free by hiredis on connect failure
            ctx->redisCtx.release();
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
        auto * ctx = static_cast<IoContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Disconnect callback: status=%d\n", status);
        ctx->running = false;

        if (ctx->channel)
        {
            ctx->channel->disableAll();
            ctx->channel->cancelAll();
        }

        // it is said that asyncCtx will auto free by hiredis on disconnect
        ctx->redisCtx.release();

        if (ctx->disconnectPromise)
            ctx->disconnectPromise->set_value();
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set disconnect callback\n");
        throw std::runtime_error("Failed to set disconnect callback");
    }

    // Step 6: Start read/write coroutines
    scheduler_->spawn([ioCtx = ioCtx_]() -> Task<> {
        NITRO_TRACE("[Redis] Read coroutine started\n");
        co_await ioCtx->channel->performRead([&](int, IoChannel *) -> IoChannel::IoStatus {
            if (!ioCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleRead(ioCtx->redisCtx.get());
            if (!ioCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            return IoChannel::IoStatus::NeedRead;
        });
        NITRO_TRACE("[Redis] Read coroutine finished\n");
    });

    scheduler_->spawn([ioCtx = ioCtx_]() -> Task<> {
        NITRO_TRACE("[Redis] Write coroutine started\n");
        co_await ioCtx->channel->performWrite([&](int, IoChannel *) -> IoChannel::IoStatus {
            if (!ioCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleWrite(ioCtx->redisCtx.get());
            if (!ioCtx->running)
            {
                return IoChannel::IoStatus::Success;
            }
            return IoChannel::IoStatus::NeedWrite;
        });
        NITRO_TRACE("[Redis] Write coroutine finished\n");
    });

    // Step 7: Wait for connection to complete
    NITRO_TRACE("[Redis] Waiting for connection to complete...\n");
    co_await ioCtx_->connectPromise->get_future().get();
    NITRO_TRACE("[Redis] Connection completed successfully\n");
    co_return;
}

Task<Result> RedisConnection::executeFormatted(const char * cmd, int len)
{
    if (!ioCtx_ || !ioCtx_->redisCtx)
        throw std::runtime_error("Not connected");

    // Create callback context
    struct CallbackContext
    {
        Promise<Result> promise;
    };

    auto ctx = std::make_unique<CallbackContext>(CallbackContext{ Promise<Result>(scheduler_) });
    auto future = ctx->promise.get_future();

    // Send command
    int ret = redisAsyncFormattedCommand(
        ioCtx_->redisCtx.get(),
        [](redisAsyncContext *, void * reply, void * privdata) {
            auto * ctx = static_cast<CallbackContext *>(privdata);
            auto * r = static_cast<redisReply *>(reply);
            if (!r)
            {
                ctx->promise.set_exception(std::make_exception_ptr(std::runtime_error("No reply")));
                return;
            }
            try
            {
                ctx->promise.set_value(Result::fromRaw(r));
            }
            catch (...)
            {
                ctx->promise.set_exception(std::current_exception());
            }
        },
        ctx.get(),
        cmd,
        len);

    if (ret != REDIS_OK)
        throw std::runtime_error("Failed to send command");

    co_return co_await future.get();
}

Task<> RedisConnection::disconnect()
{
    if (!ioCtx_ || !ioCtx_->redisCtx)
        co_return;

    co_await scheduler_->switch_to();

    if (ioCtx_->disconnecting)
        co_return;

    ioCtx_->disconnecting = true;

    ioCtx_->disconnectPromise = std::make_unique<Promise<>>(scheduler_);
    auto future = ioCtx_->disconnectPromise->get_future();

    NITRO_TRACE("[Redis] Disconnecting...\n");
    redisAsyncDisconnect(ioCtx_->redisCtx.get());

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

    if (len == -1)
        throw std::runtime_error("Failed to format command");

    return { std::unique_ptr<char, void (*)(char *)>(rawCmd, redisFreeCommand), len };
}

} // namespace nitrocoro::redis
