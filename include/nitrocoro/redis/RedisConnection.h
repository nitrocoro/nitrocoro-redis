#pragma once

#include <string>
#include <vector>
#include <nitrocoro/core/Task.h>
#include <hiredis/hiredis.h>

namespace nitrocoro::redis {

class RedisConnection {
public:
    RedisConnection(std::string host, int port);
    ~RedisConnection();

    Task<void> connect();
    Task<void> disconnect();
    Task<std::string> execute(const std::vector<std::string>& args);
    bool is_connected() const;

private:
    std::string host_;
    int port_;
    redisContext* context_{nullptr};
};

} // namespace nitrocoro::redis
