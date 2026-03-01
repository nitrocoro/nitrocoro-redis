#include <nitrocoro/redis/RedisConnection.h>
#include <stdexcept>

namespace nitrocoro::redis {

RedisConnection::RedisConnection(std::string host, int port)
    : host_(std::move(host)), port_(port) {}

RedisConnection::~RedisConnection() {
    if (context_) {
        redisFree(context_);
    }
}

Task<void> RedisConnection::connect() {
    context_ = redisConnect(host_.c_str(), port_);
    if (!context_ || context_->err) {
        std::string err = context_ ? context_->errstr : "allocation failed";
        if (context_) redisFree(context_);
        context_ = nullptr;
        throw std::runtime_error("Redis connection failed: " + err);
    }
    co_return;
}

Task<void> RedisConnection::disconnect() {
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
    co_return;
}

Task<std::string> RedisConnection::execute(const std::vector<std::string>& args) {
    if (!context_) {
        throw std::runtime_error("Not connected");
    }

    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
        argvlen.push_back(arg.size());
    }

    redisReply* reply = (redisReply*)redisCommandArgv(context_, args.size(), argv.data(), argvlen.data());
    if (!reply) {
        throw std::runtime_error("Redis command failed");
    }

    std::string result;
    if (reply->type == REDIS_REPLY_STRING) {
        result = std::string(reply->str, reply->len);
    } else if (reply->type == REDIS_REPLY_STATUS) {
        result = std::string(reply->str, reply->len);
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        result = std::to_string(reply->integer);
    } else if (reply->type == REDIS_REPLY_NIL) {
        result = "";
    } else if (reply->type == REDIS_REPLY_ERROR) {
        std::string err(reply->str, reply->len);
        freeReplyObject(reply);
        throw std::runtime_error("Redis error: " + err);
    }

    freeReplyObject(reply);
    co_return result;
}

bool RedisConnection::is_connected() const {
    return context_ != nullptr;
}

} // namespace nitrocoro::redis
