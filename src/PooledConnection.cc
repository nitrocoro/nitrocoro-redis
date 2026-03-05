/**
 * @file PooledConnection.cc
 * @brief Implementation of pooled connection with RAII semantics
 */
#include "PooledConnection.h"
#include "RedisConnectionImpl.h"
#include "PoolState.h"

namespace nitrocoro::redis
{

PooledConnection::PooledConnection(std::unique_ptr<RedisConnectionImpl> impl, std::weak_ptr<PoolState> state)
    : impl_(std::move(impl)), state_(std::move(state))
{
}

PooledConnection::~PooledConnection()
{
    if (impl_)
    {
        PoolState::returnConnection(state_, std::move(impl_));
    }
}

const std::string & PooledConnection::host() const
{
    if (!impl_)
        throw std::runtime_error("PooledConnection is empty");
    return impl_->host();
}

uint16_t PooledConnection::port() const
{
    if (!impl_)
        throw std::runtime_error("PooledConnection is empty");
    return impl_->port();
}

bool PooledConnection::isAlive() const
{
    return impl_ && impl_->isAlive();
}

Task<RedisResult> PooledConnection::executeFormatted(const char * cmd, int len)
{
    if (!impl_)
        throw std::runtime_error("PooledConnection is empty");
    co_return co_await impl_->executeFormatted(cmd, len);
}

} // namespace nitrocoro::redis
