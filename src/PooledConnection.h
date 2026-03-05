/**
 * @file PooledConnection.h
 * @brief Connection wrapper with automatic pool return
 */
#pragma once

#include <nitrocoro/redis/RedisConnection.h>

#include <memory>

namespace nitrocoro::redis
{

class RedisConnectionImpl;
struct PoolState;

class PooledConnection : public RedisConnection
{
public:
    PooledConnection(std::unique_ptr<RedisConnectionImpl> impl, std::weak_ptr<PoolState> state);
    ~PooledConnection() override;

    PooledConnection(const PooledConnection &) = delete;
    PooledConnection & operator=(const PooledConnection &) = delete;

    const std::string & host() const override;
    uint16_t port() const override;
    bool isAlive() const override;

protected:
    Task<RedisResult> executeFormatted(const char * cmd, int len) override;

private:
    std::unique_ptr<RedisConnectionImpl> impl_;
    std::weak_ptr<PoolState> state_;
};

} // namespace nitrocoro::redis
