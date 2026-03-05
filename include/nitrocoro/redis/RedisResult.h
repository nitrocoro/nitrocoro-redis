/**
 * @file RedisResult.h
 * @brief Redis command result wrapper
 */
#pragma once

#include <memory>
#include <string_view>
#include <vector>

namespace nitrocoro::redis
{

class RedisResult
{
public:
    struct Impl;
    enum class Type
    {
        String = 1,
        Array = 2,
        Integer = 3,
        Nil = 4,
        Status = 5,
        Error = 6
    };

    explicit RedisResult(std::shared_ptr<Impl> impl);
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
    std::shared_ptr<Impl> impl_;
};

} // namespace nitrocoro::redis
