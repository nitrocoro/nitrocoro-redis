/**
 * @file RedisResult.cc
 * @brief Implementation of Redis result conversion from hiredis
 */
#include <nitrocoro/redis/RedisResult.h>

#include <hiredis/hiredis.h>

#include <stdexcept>
#include <string>

namespace nitrocoro::redis
{

struct RedisResult::Impl
{
    RedisResult::Type type = RedisResult::Type::Nil;
    long long integer = 0;
    std::string str;
    std::vector<RedisResult> elements;
};

RedisResult::RedisResult()
    : impl_(std::make_shared<Impl>())
{
}

RedisResult::~RedisResult() = default;

RedisResult::RedisResult(const RedisResult &) = default;

RedisResult::RedisResult(RedisResult &&) noexcept = default;

RedisResult & RedisResult::operator=(const RedisResult &) = default;

RedisResult & RedisResult::operator=(RedisResult &&) noexcept = default;

RedisResult::RedisResult(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

RedisResult::Type RedisResult::type() const
{
    return impl_->type;
}

bool RedisResult::isString() const
{
    return impl_->type == Type::String;
}

bool RedisResult::isStatus() const
{
    return impl_->type == Type::Status;
}

bool RedisResult::isError() const
{
    return impl_->type == Type::Error;
}

bool RedisResult::isInteger() const
{
    return impl_->type == Type::Integer;
}

bool RedisResult::isArray() const
{
    return impl_->type == Type::Array;
}

bool RedisResult::isNil() const
{
    return impl_->type == Type::Nil;
}

std::string_view RedisResult::asString() const
{
    if (impl_->type != Type::String
        && impl_->type != Type::Status
        && impl_->type != Type::Error)
    {
        throw std::runtime_error("Not a string type");
    }
    return impl_->str;
}

long long RedisResult::asInteger() const
{
    if (impl_->type != Type::Integer)
        throw std::runtime_error("Not an integer type");
    return impl_->integer;
}

const std::vector<RedisResult> & RedisResult::asArray() const
{
    if (impl_->type != Type::Array)
        throw std::runtime_error("Not an array type");
    return impl_->elements;
}

namespace detail
{

RedisResult redisReplayToRedisResultImpl(const redisReply * r)
{
    if (!r)
        throw std::runtime_error("Null reply");

    auto impl = std::make_shared<RedisResult::Impl>();
    impl->type = static_cast<RedisResult::Type>(r->type);

    switch (r->type)
    {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS:
        case REDIS_REPLY_ERROR:
            impl->str.assign(r->str, r->len);
            break;
        case REDIS_REPLY_INTEGER:
            impl->integer = r->integer;
            break;
        case REDIS_REPLY_ARRAY:
            impl->elements.reserve(r->elements);
            for (size_t i = 0; i < r->elements; ++i)
                impl->elements.push_back(redisReplayToRedisResultImpl(r->element[i]));
            break;
        case REDIS_REPLY_NIL:
            break;
        default:
            throw std::runtime_error("Unknown reply type");
    }

    return RedisResult(std::move(impl));
}

} // namespace detail

} // namespace nitrocoro::redis
