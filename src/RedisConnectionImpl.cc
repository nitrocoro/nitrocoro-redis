/**
 * @file RedisConnectionImpl.cc
 * @brief Implementation of Redis connection with hiredis integration
 */
#include "RedisConnectionImpl.h"

#include <nitrocoro/utils/Debug.h>

#include <stdexcept>

namespace nitrocoro::redis
{

using nitrocoro::Promise;
using nitrocoro::Scheduler;
using nitrocoro::Task;
using nitrocoro::TriggerMode;
using nitrocoro::io::Channel;

namespace detail
{
RedisResult redisReplayToRedisResultImpl(const redisReply * r);
}

struct ConnectionContext
{
    RedisAsyncContextPtr redisCtx;
    std::unique_ptr<io::Channel> channel;
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
    auto channel = std::make_unique<Channel>(redisCtx->c.fd, TriggerMode::LevelTriggered, scheduler);
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
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)", status, status == REDIS_OK ? "OK" : "ERROR");
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s", c->errstr);
            void * _ = ctx->redisCtx.release();
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

        if (ctx->channel)
        {
            ctx->channel->disableAll();
            ctx->channel->cancelAll();
        }

        [[maybe_unused]] void * _ = ctx->redisCtx.release();

        if (ctx->disconnectPromise)
            ctx->disconnectPromise->set_value();
    });
    if (ret != REDIS_OK)
    {
        NITRO_ERROR("[Redis] Failed to set disconnect callback");
        throw std::runtime_error("Failed to set disconnect callback");
    }

    // Step 6: Start read/write coroutines
    scheduler->spawn([connCtx]() -> Task<> {
        NITRO_TRACE("[Redis] Read coroutine started");
        co_await connCtx->channel->performRead([&](int, Channel *) -> Channel::IoStatus {
            if (connCtx->state == ConnectionContext::State::Disconnected)
                return Channel::IoStatus::Success;
            redisAsyncHandleRead(connCtx->redisCtx.get());
            if (connCtx->state == ConnectionContext::State::Disconnected)
                return Channel::IoStatus::Success;
            return Channel::IoStatus::NeedRead;
        });
        NITRO_TRACE("[Redis] Read coroutine finished");
    });

    scheduler->spawn([connCtx]() -> Task<> {
        NITRO_TRACE("[Redis] Write coroutine started");
        // hack: reset the initial writable flag
        co_await connCtx->channel->perform([&](int, Channel *) -> Channel::IoStatus {
            return Channel::IoStatus::ExhaustWrite;
        });

        co_await connCtx->channel->performWrite([&](int, Channel *) -> Channel::IoStatus {
            if (connCtx->state == ConnectionContext::State::Disconnected)
                return Channel::IoStatus::Success;
            redisAsyncHandleWrite(connCtx->redisCtx.get());
            if (connCtx->state == ConnectionContext::State::Disconnected)
                return Channel::IoStatus::Success;
            return Channel::IoStatus::NeedWrite;
        });
        NITRO_TRACE("[Redis] Write coroutine finished");
    });

    // Step 7: Wait for connection to complete
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
    if (!ctx_)
        throw std::runtime_error("RedisConnection is not connected");
    return ctx_->host;
}

uint16_t RedisConnectionImpl::port() const
{
    if (!ctx_)
        throw std::runtime_error("RedisConnection is not connected");
    return ctx_->port;
}

bool RedisConnectionImpl::isAlive() const
{
    return ctx_ && ctx_->state == ConnectionContext::State::Connected;
}

Task<RedisResult> RedisConnectionImpl::executeFormatted(const char * cmd, int len)
{
    if (!ctx_)
        throw std::runtime_error("RedisConnection is not connected");
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
