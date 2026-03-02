#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/io/IoChannel.h>

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
    Task<std::string> execute(const std::vector<std::string> & args);

private:
    struct IoContext;

    std::string host_;
    int port_;
    Scheduler * scheduler_;
    std::shared_ptr<IoContext> ioCtx_;
};

} // namespace nitrocoro::redis
