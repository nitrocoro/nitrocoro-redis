#include <nitrocoro/redis/RedisClient.h>

namespace nitrocoro::redis {

RedisClient::RedisClient(std::string host, int port)
    : connection_(std::make_unique<RedisConnection>(std::move(host), port)) {}

RedisClient::~RedisClient() {}

Task<void> RedisClient::connect() {
    co_await connection_->connect();
}

Task<std::string> RedisClient::get(const std::string& key) {
    std::vector<std::string> args = {"GET", key};
    co_return co_await connection_->execute(args);
}

Task<void> RedisClient::set(const std::string& key, const std::string& value) {
    std::vector<std::string> args = {"SET", key, value};
    co_await connection_->execute(args);
}

Task<void> RedisClient::del(const std::string& key) {
    std::vector<std::string> args = {"DEL", key};
    co_await connection_->execute(args);
}

} // namespace nitrocoro::redis
