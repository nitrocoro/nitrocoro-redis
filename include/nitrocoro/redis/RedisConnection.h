#pragma once

#include <nitrocoro/core/Scheduler.h>
#include <nitrocoro/core/Task.h>
#include <nitrocoro/redis/RedisResult.h>

#include <array>
#include <memory>
#include <string>
#include <tuple>
#include <utility>

namespace nitrocoro::redis
{

class RedisConnection
{
public:
    static Task<RedisConnection> connect(std::string host, uint16_t port, Scheduler * scheduler = Scheduler::current());

    RedisConnection();
    ~RedisConnection();
    RedisConnection(const RedisConnection &) = delete;
    RedisConnection & operator=(const RedisConnection &) = delete;
    RedisConnection(RedisConnection &&) = default;
    RedisConnection & operator=(RedisConnection &&) = default;

    // TODO: how to inform and handle broken connection?
    Task<> disconnect();

    const std::string & host() const;
    uint16_t port() const;

    explicit operator bool() const noexcept;

    template <typename... Args>
    Task<RedisResult> execute(const char * format, Args &&... args)
    {
        auto [cmd, len] = formatCommand(format, std::forward<Args>(args)...);
        co_return co_await executeFormatted(cmd.get(), len);
    }

    template <typename... Keys, typename... Args>
    Task<RedisResult> eval(const std::string & script,
                      std::tuple<Keys...> keys,
                      std::tuple<Args...> args = {})
    {
        co_return co_await evalImpl(script, keys, args,
                                    std::index_sequence_for<Keys...>{},
                                    std::index_sequence_for<Args...>{});
    }

private:
    struct ConnectionContext;
    explicit RedisConnection(std::shared_ptr<ConnectionContext> ctx);

    static std::pair<std::unique_ptr<char, void (*)(char *)>, int> formatCommand(const char * format, ...);
    Task<RedisResult> executeFormatted(const char * cmd, int len);

    template <size_t N>
    static constexpr auto buildEvalFormat()
    {
        constexpr char prefix[] = "EVAL %s %d";
        constexpr char suffix[] = " %s";
        std::array<char, sizeof(prefix) + N * 3> result = {};

        size_t pos = 0;
        for (size_t i = 0; i < sizeof(prefix) - 1; ++i)
            result[pos++] = prefix[i];
        for (size_t i = 0; i < N; ++i)
            for (size_t j = 0; j < sizeof(suffix) - 1; ++j)
                result[pos++] = suffix[j];
        result[pos] = '\0';
        return result;
    }

    template <typename... Keys, typename... Args, size_t... KeyIdx, size_t... ArgIdx>
    Task<RedisResult> evalImpl(const std::string & script,
                          const std::tuple<Keys...> & keys,
                          const std::tuple<Args...> & args,
                          std::index_sequence<KeyIdx...>,
                          std::index_sequence<ArgIdx...>)
    {
        constexpr size_t numKeys = sizeof...(Keys);
        constexpr size_t numArgs = sizeof...(Args);
        constexpr auto fmt = buildEvalFormat<numKeys + numArgs>();

        co_return co_await execute(fmt.data(), script.c_str(), static_cast<int>(numKeys),
                                   std::get<KeyIdx>(keys)..., std::get<ArgIdx>(args)...);
    }

    std::shared_ptr<ConnectionContext> ctx_;
};

} // namespace nitrocoro::redis
