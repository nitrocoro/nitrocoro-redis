#include <nitrocoro/core/Future.h>
#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/utils/Debug.h>

#include <hiredis/async.h>

#include <cstring>
#include <errno.h>
#include <stdexcept>

namespace nitrocoro::redis
{

RedisConnection::RedisConnection(redisAsyncContext * ctx, std::unique_ptr<IoChannel> channel)
    : redisCtx_(ctx), channel_(std::move(channel)) {}

RedisConnection::~RedisConnection()
{
    channel_->disableAll();
}

Task<std::shared_ptr<RedisConnection>> RedisConnection::connect(
    std::string host, int port, Scheduler * scheduler)
{
    NITRO_TRACE("[Redis] Connecting to %s:%d\n", host.c_str(), port);

    // Step 1: Create async connection
    redisAsyncContext * asyncCtx = redisAsyncConnect(host.c_str(), port);
    if (!asyncCtx || asyncCtx->err)
    {
        std::string err = asyncCtx ? asyncCtx->errstr : "allocation failed";
        NITRO_ERROR("[Redis] redisAsyncConnect failed: %s\n", err.c_str());
        throw std::runtime_error("Redis connection failed: " + err);
    }
    NITRO_TRACE("[Redis] redisAsyncConnect created, fd=%d\n", asyncCtx->c.fd);

    // Step 2: Switch to scheduler thread
    co_await scheduler->switch_to();

    // Step 3: Create IoChannel to monitor socket
    auto channel = std::make_unique<IoChannel>(asyncCtx->c.fd, TriggerMode::LevelTriggered, scheduler);

    // Step 3.5: Create temporary context to store necessary data
    struct ConnectContext
    {
        IoChannel * channel;
        Promise<> * promise;
        std::atomic<bool> done{ false };
    };
    Promise<> connectPromise(scheduler);
    auto ctx = std::make_unique<ConnectContext>();
    ctx->channel = channel.get();
    ctx->promise = &connectPromise;

    // Step 4: Setup hiredis event hooks
    asyncCtx->ev.addRead = [](void * privdata) {
        auto * ctx = static_cast<ConnectContext *>(privdata);
        ctx->channel->enableReading();
    };
    asyncCtx->ev.delRead = [](void * privdata) {
        auto * ctx = static_cast<ConnectContext *>(privdata);
        ctx->channel->disableReading();
    };
    asyncCtx->ev.addWrite = [](void * privdata) {
        auto * ctx = static_cast<ConnectContext *>(privdata);
        ctx->channel->enableWriting();
    };
    asyncCtx->ev.delWrite = [](void * privdata) {
        auto * ctx = static_cast<ConnectContext *>(privdata);
        ctx->channel->disableWriting();
    };
    asyncCtx->ev.data = ctx.get();

    // Step 5: Register connection complete callback
    redisAsyncSetConnectCallback(asyncCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<ConnectContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)\n", status, status == REDIS_OK ? "OK" : "ERROR");
        ctx->done = true;
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s\n", c->errstr);
            ctx->promise->set_exception(std::make_exception_ptr(std::runtime_error(c->errstr)));
        }
        else
        {
            NITRO_TRACE("[Redis] Connection successful\n");
            ctx->promise->set_value();
        }
    });
    NITRO_TRACE("[Redis] Connect callback registered\n");

    // Step 6: Start read/write coroutines to drive IO
    channel->enableReading();
    NITRO_TRACE("[Redis] Starting IO coroutines\n");

    scheduler->spawn([asyncCtx, ctxPtr = ctx.get()]() -> Task<> {
        NITRO_TRACE("[Redis] Read coroutine started\n");
        co_await ctxPtr->channel->performRead([asyncCtx, ctxPtr](int, IoChannel *) -> IoChannel::IoStatus {
            if (ctxPtr->done)
            {
                NITRO_TRACE("[Redis] Read coroutine done\n");
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleRead(asyncCtx);
            return IoChannel::IoStatus::NeedRead;
        });
        NITRO_TRACE("[Redis] Read coroutine finished\n");
    });

    scheduler->spawn([asyncCtx, ctxPtr = ctx.get()]() -> Task<> {
        NITRO_TRACE("[Redis] Write coroutine started\n");
        co_await ctxPtr->channel->performWrite([asyncCtx, ctxPtr](int, IoChannel *) -> IoChannel::IoStatus {
            if (ctxPtr->done)
            {
                NITRO_TRACE("[Redis] Write coroutine done\n");
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleWrite(asyncCtx);
            return IoChannel::IoStatus::NeedWrite;
        });
        NITRO_TRACE("[Redis] Write coroutine finished\n");
    });

    // Step 7: Wait for connection to complete
    NITRO_TRACE("[Redis] Waiting for connection to complete...\n");
    co_await connectPromise.get_future().get();
    NITRO_TRACE("[Redis] Connection completed successfully\n");

    // Step 8: Return connection object
    co_return std::shared_ptr<RedisConnection>(new RedisConnection(asyncCtx, std::move(channel)));
}

Task<std::string> RedisConnection::execute(const std::vector<std::string> & args)
{
    co_return "aaa";

    // if (args.empty())
    // {
    //     throw std::runtime_error("Empty command");
    // }
    //
    // std::vector<const char *> argv;
    // std::vector<size_t> argvlen;
    // for (const auto & arg : args)
    // {
    //     argv.push_back(arg.c_str());
    //     argvlen.push_back(arg.size());
    // }
    //
    // int ret = redisAppendCommandArgv(redisCtx_.get(), args.size(), argv.data(), argvlen.data());
    // if (ret != REDIS_OK)
    // {
    //     throw std::runtime_error("Failed to append command");
    // }
    //
    // auto writeResult = co_await channel_->performWrite([this](int, IoChannel * ch) -> IoChannel::IoStatus {
    //     int done = 0;
    //     if (redisBufferWrite(redisCtx_.get(), &done) == REDIS_ERR)
    //     {
    //         return IoChannel::IoStatus::Error;
    //     }
    //     if (done)
    //     {
    //         ch->disableWriting();
    //         return IoChannel::IoStatus::Success;
    //     }
    //     ch->enableWriting();
    //     return IoChannel::IoStatus::NeedWrite;
    // });
    //
    // if (writeResult != IoChannel::IoResult::Success)
    // {
    //     throw std::runtime_error("Failed to write command");
    // }
    //
    // redisReply * reply = nullptr;
    // auto readResult = co_await channel_->performRead([this, &reply](int, IoChannel *) -> IoChannel::IoStatus {
    //     if (redisBufferRead(redisCtx_.get()) == REDIS_ERR)
    //     {
    //         return IoChannel::IoStatus::Error;
    //     }
    //     if (redisGetReply(redisCtx_.get(), (void **)&reply) == REDIS_OK && reply)
    //     {
    //         return IoChannel::IoStatus::Success;
    //     }
    //     return IoChannel::IoStatus::NeedRead;
    // });
    //
    // if (readResult != IoChannel::IoResult::Success)
    // {
    //     throw std::runtime_error("Failed to read reply");
    // }
    //
    // if (!reply)
    // {
    //     throw std::runtime_error("No reply received");
    // }
    //
    // std::string result;
    // if (reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS)
    // {
    //     result = std::string(reply->str, reply->len);
    // }
    // else if (reply->type == REDIS_REPLY_INTEGER)
    // {
    //     result = std::to_string(reply->integer);
    // }
    // else if (reply->type == REDIS_REPLY_NIL)
    // {
    //     result = "";
    // }
    // else if (reply->type == REDIS_REPLY_ERROR)
    // {
    //     std::string err(reply->str, reply->len);
    //     freeReplyObject(reply);
    //     throw std::runtime_error("Redis error: " + err);
    // }
    //
    // freeReplyObject(reply);
    // co_return result;
}

} // namespace nitrocoro::redis
