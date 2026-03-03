#pragma once

#include <memory>
#include <string_view>
#include <vector>

namespace nitrocoro::redis
{

class RedisConnection;

class RedisResult
{
public:
    enum class Type
    {
        String = 1,
        Array = 2,
        Integer = 3,
        Nil = 4,
        Status = 5,
        Error = 6
    };

    RedisResult();
    ~RedisResult();
    RedisResult(const RedisResult &);
    RedisResult(RedisResult &&) noexcept;
    RedisResult & operator=(const RedisResult &);
    RedisResult & operator=(RedisResult &&) noexcept;

    Type type() const;
    bool isString() const;
    bool isStatus() const;
    bool isError() const;
    bool isInteger() const;
    bool isArray() const;
    bool isNil() const;

    std::string_view asString() const;
    long long asInteger() const;
    const std::vector<RedisResult> & asArray() const;

private:
    friend class RedisConnection;
    static RedisResult fromRaw(const void * rawReply);

    struct Impl;
    std::shared_ptr<Impl> impl_;

    explicit RedisResult(std::shared_ptr<Impl> impl);
};

} // namespace nitrocoro::redis