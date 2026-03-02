#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/redis/Result.h>

#include <memory>
#include <string>

struct redisAsyncContext;

namespace nitrocoro::redis
{

class RedisConnection
{
public:
    RedisConnection(std::string host, int port, Scheduler * scheduler = Scheduler::current());
    ~RedisConnection();

    Task<> connect();
    Task<> disconnect();

    template <typename... Args>
    Task<Result> execute(const char * format, Args &&... args)
    {
        auto [cmd, len] = formatCommand(format, std::forward<Args>(args)...);
        co_return co_await executeFormatted(cmd.get(), len);
    }

private:
    static std::pair<std::unique_ptr<char, void (*)(char *)>, int> formatCommand(const char * format, ...);
    Task<Result> executeFormatted(const char * cmd, int len);

    struct IoContext;

    std::string host_;
    int port_;
    Scheduler * scheduler_;
    std::shared_ptr<IoContext> ioCtx_;
};

} // namespace nitrocoro::redis
