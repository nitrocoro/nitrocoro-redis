#pragma once

#include <string>
#include <memory>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/redis/RedisConnection.h>

namespace nitrocoro::redis {

class RedisClient {
public:
    RedisClient(std::string host, int port = 6379);
    ~RedisClient();
    RedisClient(RedisClient&&) = default;
    RedisClient& operator=(RedisClient&&) = default;
    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    Task<void> connect();
    Task<std::string> get(const std::string& key);
    Task<void> set(const std::string& key, const std::string& value);
    Task<void> del(const std::string& key);

private:
    std::unique_ptr<RedisConnection> connection_;
};

} // namespace nitrocoro::redis
