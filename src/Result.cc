#include <nitrocoro/redis/Result.h>

#include <hiredis/hiredis.h>

#include <stdexcept>
#include <string>

namespace nitrocoro::redis
{

struct Result::Impl
{
    Result::Type type = Result::Type::Nil;
    long long integer = 0;
    std::string str;
    std::vector<Result> elements;
};

Result::Result()
    : impl_(std::make_shared<Impl>()) {}

Result::~Result() = default;

Result::Result(const Result &) = default;

Result::Result(Result &&) noexcept = default;

Result & Result::operator=(const Result &) = default;

Result & Result::operator=(Result &&) noexcept = default;

Result::Result(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Result::Type Result::type() const { return impl_->type; }

bool Result::isString() const { return impl_->type == Type::String; }

bool Result::isStatus() const { return impl_->type == Type::Status; }

bool Result::isError() const { return impl_->type == Type::Error; }

bool Result::isInteger() const { return impl_->type == Type::Integer; }

bool Result::isArray() const { return impl_->type == Type::Array; }

bool Result::isNil() const { return impl_->type == Type::Nil; }

std::string_view Result::asString() const
{
    if (impl_->type != Type::String
        && impl_->type != Type::Status
        && impl_->type != Type::Error)
    {
        throw std::runtime_error("Not a string type");
    }
    return impl_->str;
}

long long Result::asInteger() const
{
    if (impl_->type != Type::Integer)
        throw std::runtime_error("Not an integer type");
    return impl_->integer;
}

const std::vector<Result> & Result::asArray() const
{
    if (impl_->type != Type::Array)
        throw std::runtime_error("Not an array type");
    return impl_->elements;
}

Result Result::fromRaw(const void * rawReply)
{
    auto * r = static_cast<const redisReply *>(rawReply);
    if (!r)
        throw std::runtime_error("Null reply");

    auto impl = std::make_shared<Impl>();
    impl->type = static_cast<Result::Type>(r->type);

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
                impl->elements.push_back(fromRaw(r->element[i]));
            break;
        case REDIS_REPLY_NIL:
            break;
        default:
            throw std::runtime_error("Unknown reply type");
    }

    return Result(std::move(impl));
}

} // namespace nitrocoro::redis
