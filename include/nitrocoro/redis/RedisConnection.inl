#pragma once

#include <hiredis/async.h>
#include <memory>

namespace nitrocoro::redis
{

template <typename... Args>
Task<std::string> RedisConnection::execute(const char * format, Args &&... args)
{
    // Format command
    char * rawCmd = nullptr;
    int len = redisFormatCommand(&rawCmd, format, std::forward<Args>(args)...);

    if (len == -1)
        throw std::runtime_error("Failed to format command");

    struct CmdDeleter
    {
        void operator()(char * p) const { redisFreeCommand(p); }
    };
    std::unique_ptr<char, CmdDeleter> cmd(rawCmd);

    auto result = co_await executeFormatted(cmd.get(), len);
    co_return result;
}

} // namespace nitrocoro::redis
