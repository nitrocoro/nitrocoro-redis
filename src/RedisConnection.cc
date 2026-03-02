#include <nitrocoro/core/Future.h>
#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/utils/Debug.h>

#include <hiredis/async.h>

#include <cstdarg>
#include <cstring>
#include <errno.h>
#include <stdexcept>

namespace nitrocoro::redis
{

struct RedisConnection::IoContext
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

    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<IoChannel> channel;
    bool running{ true };
    bool disconnecting{ false };
    std::unique_ptr<Promise<>> connectPromise;
    std::unique_ptr<Promise<>> disconnectPromise;

    ~IoContext()
    {
        // Channel disableAll already called in disconnect callback
    }
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
    IoContext::RedisAsyncContextPtr redisCtxPtr(redisAsyncConnect(host_.c_str(), port_));
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

Task<std::string> RedisConnection::executeFormatted(const char * cmd, int len)
{
    if (!ioCtx_ || !ioCtx_->redisCtx)
        throw std::runtime_error("Not connected");

    // Create callback context
    struct CallbackContext
    {
        Promise<std::string> promise;
    };

    auto ctx = std::make_unique<CallbackContext>(CallbackContext{ Promise<std::string>(scheduler_) });
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
            if (r->type == REDIS_REPLY_ERROR)
            {
                ctx->promise.set_exception(
                    std::make_exception_ptr(std::runtime_error(std::string(r->str, r->len))));
            }
            else if (r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_STATUS)
            {
                ctx->promise.set_value(std::string(r->str, r->len));
            }
            else if (r->type == REDIS_REPLY_INTEGER)
            {
                ctx->promise.set_value(std::to_string(r->integer));
            }
            else if (r->type == REDIS_REPLY_NIL)
            {
                ctx->promise.set_value("");
            }
            else
            {
                ctx->promise.set_exception(
                    std::make_exception_ptr(std::runtime_error("Unsupported reply type")));
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

} // namespace nitrocoro::redis
