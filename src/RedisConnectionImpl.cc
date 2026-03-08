/**
 * @file RedisConnectionImpl.cc
 * @brief Implementation of Redis connection with hiredis integration
 */
#include "RedisConnectionImpl.h"

#include <nitrocoro/io/CallbackChannel.h>
#include <nitrocoro/utils/Debug.h>

#include <stdexcept>

namespace nitrocoro::redis
{

using nitrocoro::Promise;
using nitrocoro::Scheduler;
using nitrocoro::Task;

namespace detail
{
RedisResult redisReplayToRedisResultImpl(const redisReply * r);
}

struct ConnectionContext
{
    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<io::CallbackChannel> channel;
    std::string host;
    uint16_t port;
    Scheduler * scheduler;
    enum class State
    {
        Connected,
        Disconnecting,
        Disconnected
    };
    State state{ State::Connected };
    std::unique_ptr<Promise<>> connectPromise;
    std::unique_ptr<Promise<>> disconnectPromise;
};

Task<std::unique_ptr<RedisConnection>> RedisConnection::connect(std::string host, uint16_t port, Scheduler * scheduler)
{
    NITRO_TRACE("[Redis] Connecting to %s:%hu", host.c_str(), port);

    // Step 1: Create async connection
    RedisAsyncContextPtr redisCtxPtr(redisAsyncConnect(host.c_str(), port));
    auto * redisCtx = redisCtxPtr.get();
    if (!redisCtx || redisCtx->err)
    {
        std::string err = redisCtx ? redisCtx->errstr : "allocation failed";
        NITRO_ERROR("[Redis] redisAsyncConnect failed: %s", err.c_str());
        throw std::runtime_error("Redis connection failed: " + err);
    }
    NITRO_TRACE("[Redis] redisAsyncConnect created, fd=%d", redisCtx->c.fd);

    // Step 2: Switch to scheduler thread
    co_await scheduler->switch_to();

    // Step 3: Create IO context with all resources
    auto channel = std::make_unique<io::CallbackChannel>(redisCtx->c.fd, scheduler);
    auto connCtx = std::make_shared<ConnectionContext>(ConnectionContext{
        .redisCtx = std::move(redisCtxPtr),
        .channel = std::move(channel),
        .host = std::move(host),
        .port = port,
        .scheduler = scheduler,
        .connectPromise = std::make_unique<Promise<>>(scheduler) });

    // Step 4: Setup hiredis event hooks
    redisCtx->ev.data = connCtx.get();
    redisCtx->ev.addRead = [](void * p) { static_cast<ConnectionContext *>(p)->channel->enableReading(); };
    redisCtx->ev.delRead = [](void * p) { static_cast<ConnectionContext *>(p)->channel->disableReading(); };
    redisCtx->ev.addWrite = [](void * p) { static_cast<ConnectionContext *>(p)->channel->enableWriting(); };
    redisCtx->ev.delWrite = [](void * p) { static_cast<ConnectionContext *>(p)->channel->disableWriting(); };

    std::weak_ptr<ConnectionContext> weakCtx = connCtx;
    connCtx->channel->setReadableCallback([weakCtx]() {
        if (auto ctx = weakCtx.lock(); ctx && ctx->redisCtx)
            redisAsyncHandleRead(ctx->redisCtx.get());
    });
    connCtx->channel->setWritableCallback([weakCtx]() {
        if (auto ctx = weakCtx.lock(); ctx && ctx->redisCtx)
            redisAsyncHandleWrite(ctx->redisCtx.get());
    });

    // Step 5: Register callbacks
    int ret = redisAsyncSetConnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<ConnectionContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)", status, status == REDIS_OK ? "OK" : "ERROR");
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s", c->errstr);
            void * _ = ctx->redisCtx.release();
            ctx->channel->disableAll(); // must remove from epoll before closing fd
            ctx->connectPromise->set_exception(std::make_exception_ptr(std::runtime_error(c->errstr)));
        }
        else
        {
            NITRO_TRACE("[Redis] Connection successful");
            ctx->connectPromise->set_value();
        }
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set connect callback");
        throw std::runtime_error("Failed to set connect callback");
    }

    ret = redisAsyncSetDisconnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<ConnectionContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Disconnect callback: status=%d", status);
        ctx->state = ConnectionContext::State::Disconnected;
        auto * redisCtx = ctx->redisCtx.release(); // hiredis will free this after the callback returns
        redisCtx->ev.addWrite = nullptr;
        redisCtx->ev.delWrite = nullptr;
        redisCtx->ev.addRead = nullptr;
        redisCtx->ev.delRead = nullptr;
        redisCtx->ev.cleanup = nullptr;
        redisCtx->ev.data = nullptr;
        ctx->channel->disableAll();

        if (ctx->disconnectPromise)
            ctx->disconnectPromise->set_value();
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set disconnect callback");
        throw std::runtime_error("Failed to set disconnect callback");
    }

    // Step 6: Wait for connection to complete
    NITRO_TRACE("[Redis] Waiting for connection to complete...");
    co_await connCtx->connectPromise->get_future().get();
    NITRO_TRACE("[Redis] Connection completed successfully");
    co_return std::make_unique<RedisConnectionImpl>(std::move(connCtx));
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

RedisConnectionImpl::RedisConnectionImpl(std::shared_ptr<ConnectionContext> ctx)
    : ctx_(std::move(ctx))
{
}

RedisConnectionImpl::~RedisConnectionImpl()
{
    if (!ctx_)
        return;
    ctx_->scheduler->dispatch([ctx = std::move(ctx_)]() {
        if (ctx->state != ConnectionContext::State::Connected)
            return;
        ctx->state = ConnectionContext::State::Disconnecting;

        if (ctx->redisCtx)
            redisAsyncDisconnect(ctx->redisCtx.get());
    });
}

const std::string & RedisConnectionImpl::host() const
{
    return ctx_->host;
}

uint16_t RedisConnectionImpl::port() const
{
    return ctx_->port;
}

bool RedisConnectionImpl::isAlive() const
{
    return ctx_ && ctx_->state == ConnectionContext::State::Connected;
}

Task<RedisResult> RedisConnectionImpl::executeFormatted(const char * cmd, int len)
{
    if (ctx_->state != ConnectionContext::State::Connected)
        throw std::runtime_error("Connection is not available");

    struct CallbackContext
    {
        Promise<RedisResult> promise;
    };

    auto cbCtxPtr = std::make_unique<CallbackContext>(CallbackContext{ Promise<RedisResult>(ctx_->scheduler) });
    auto future = cbCtxPtr->promise.get_future();

    int ret = redisAsyncFormattedCommand(
        ctx_->redisCtx.get(),
        [](redisAsyncContext *, void * reply, void * privdata) {
            auto * cbCtx = static_cast<CallbackContext *>(privdata);
            auto * r = static_cast<redisReply *>(reply);
            if (!r)
            {
                cbCtx->promise.set_exception(std::make_exception_ptr(std::runtime_error("Connection lost")));
                return;
            }
            try
            {
                cbCtx->promise.set_value(detail::redisReplayToRedisResultImpl(r));
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

} // namespace nitrocoro::redis
