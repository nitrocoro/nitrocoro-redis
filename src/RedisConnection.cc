#include <nitrocoro/core/Future.h>
#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/utils/Debug.h>

#include <hiredis/async.h>

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
    std::atomic<bool> running{ true };
    std::unique_ptr<Promise<>> connectPromise;

    ~IoContext()
    {
        if (channel)
            channel->disableAll();
    }
};

RedisConnection::RedisConnection(std::string host, int port, Scheduler * scheduler)
    : host_(std::move(host)), port_(port), scheduler_(scheduler)
{
}

RedisConnection::~RedisConnection()
{
    if (ioCtx_)
    {
        ioCtx_->running = false;
        if (ioCtx_->channel)
            ioCtx_->channel->cancelAll();
    }
}

Task<void> RedisConnection::connect()
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

    // Step 5: Register connection complete callback
    int ret = redisAsyncSetConnectCallback(redisCtx, [](const redisAsyncContext * c, int status) {
        auto * ctx = static_cast<IoContext *>(c->ev.data);
        NITRO_TRACE("[Redis] Connect callback: status=%d (%s)\n", status, status == REDIS_OK ? "OK" : "ERROR");
        if (status != REDIS_OK)
        {
            NITRO_ERROR("[Redis] Connection failed: %s\n", c->errstr);
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
    NITRO_TRACE("[Redis] Connect callback registered\n");

    // Step 6: Start read/write coroutines
    ioCtx_->channel->enableReading();
    NITRO_TRACE("[Redis] Starting IO coroutines\n");

    scheduler_->spawn([ioCtx = ioCtx_]() -> Task<> {
        NITRO_TRACE("[Redis] Read coroutine started\n");
        co_await ioCtx->channel->performRead([ioCtx](int, IoChannel *) -> IoChannel::IoStatus {
            if (!ioCtx->running)
            {
                NITRO_TRACE("[Redis] Read coroutine stopping\n");
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleRead(ioCtx->redisCtx.get());
            return IoChannel::IoStatus::NeedRead;
        });
        NITRO_TRACE("[Redis] Read coroutine finished\n");
    });

    scheduler_->spawn([ioCtx = ioCtx_]() -> Task<> {
        NITRO_TRACE("[Redis] Write coroutine started\n");
        co_await ioCtx->channel->performWrite([ioCtx](int, IoChannel *) -> IoChannel::IoStatus {
            if (!ioCtx->running)
            {
                NITRO_TRACE("[Redis] Write coroutine stopping\n");
                return IoChannel::IoStatus::Success;
            }
            redisAsyncHandleWrite(ioCtx->redisCtx.get());
            return IoChannel::IoStatus::NeedWrite;
        });
        NITRO_TRACE("[Redis] Write coroutine finished\n");
    });

    // Step 7: Wait for connection to complete
    NITRO_TRACE("[Redis] Waiting for connection to complete...\n");
    co_await ioCtx_->connectPromise->get_future().get();
    NITRO_TRACE("[Redis] Connection completed successfully\n");

    // Step 8: Connection completed
    co_return;
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
