#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/io/IoChannel.h>
#include <nitrocoro/redis/Result.h>

#include <memory>
#include <string>
#include <vector>

struct redisAsyncContext;

namespace nitrocoro::redis
{

using nitrocoro::Scheduler;
using nitrocoro::Task;
using nitrocoro::io::IoChannel;

class RedisConnection
{
public:
    RedisConnection(std::string host, int port, Scheduler * scheduler = Scheduler::current());
    ~RedisConnection();

    Task<> connect();
    Task<> disconnect();

    template <typename... Args>
    Task<Result> execute(const char * format, Args &&... args);

private:
    Task<Result> executeFormatted(const char * cmd, int len);

    struct IoContext;

    std::string host_;
    int port_;
    Scheduler * scheduler_;
    std::shared_ptr<IoContext> ioCtx_;
};

} // namespace nitrocoro::redis

#include "RedisConnection.inl"
