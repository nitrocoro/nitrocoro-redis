#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/io/IoChannel.h>
#include <memory>
#include <string>
#include <vector>

struct redisAsyncContext;

namespace nitrocoro::redis {

using nitrocoro::Scheduler;
using nitrocoro::Task;
using nitrocoro::io::IoChannel;

class RedisConnection {
public:
    static Task<std::shared_ptr<RedisConnection>> connect(
        std::string host, 
        int port,
        Scheduler* scheduler = Scheduler::current());

    ~RedisConnection();

    Task<std::string> execute(const std::vector<std::string>& args);

private:
    RedisConnection(redisAsyncContext* ctx, std::unique_ptr<IoChannel> channel);

    redisAsyncContext* redisCtx_;
    std::unique_ptr<IoChannel> channel_;
};

} // namespace nitrocoro::redis
