#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/testing/Test.h>

#include <cstdlib>

using namespace nitrocoro;
using namespace nitrocoro::redis;

static std::string getHost()
{
    const char * env = std::getenv("REDIS_HOST");
    return env ? env : "127.0.0.1";
}

static int getPort()
{
    const char * env = std::getenv("REDIS_PORT");
    return env ? std::stoi(env) : 6379;
}

NITRO_TEST(test_redis_client)
{
    NITRO_INFO("Testing RedisClient");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());
    NITRO_INFO("Connected to Redis");

    // Test host() and port() methods
    NITRO_CHECK(conn->host() == getHost());
    NITRO_CHECK(conn->port() == getPort());
    NITRO_INFO("Connection info: %s:%d", conn->host().c_str(), conn->port());

    // Test SET
    auto setResult = co_await conn->execute("SET %s %s", "test_key", "test_value");
    NITRO_INFO("SET result: %s", std::string(setResult.asString()).c_str());
    NITRO_CHECK(setResult.isStatus() && setResult.asString() == "OK");

    // Test GET
    auto getResult = co_await conn->execute("GET %s", "test_key");
    NITRO_INFO("GET result: %s", std::string(getResult.asString()).c_str());
    NITRO_CHECK(getResult.isString() && getResult.asString() == "test_value");

    // Test INCR
    auto incrResult = co_await conn->execute("INCR %s", "counter");
    NITRO_INFO("INCR result: %lld", incrResult.asInteger());
    NITRO_CHECK(incrResult.isInteger());

    // Test DEL
    auto delResult = co_await conn->execute("DEL %s %s", "test_key", "counter");
    NITRO_INFO("DEL result: %lld", delResult.asInteger());
    NITRO_CHECK(delResult.isInteger() && delResult.asInteger() == 2);

    NITRO_INFO("All tests passed");
    co_return;
}

NITRO_TEST(test_redis_auto_disconnect)
{
    NITRO_INFO("Testing auto disconnect");

    {
        auto conn = co_await RedisConnection::connect(getHost(), getPort());
        NITRO_INFO("Connected to Redis");

        auto setResult = co_await conn->execute("SET %s %s", "auto_key", "auto_value");
        NITRO_CHECK(setResult.isStatus() && setResult.asString() == "OK");

        // conn will be destroyed here, triggering auto disconnect
    }

    NITRO_INFO("Auto disconnect test passed");
    co_return;
}

NITRO_TEST(test_redis_eval)
{
    NITRO_INFO("Testing Redis EVAL");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());

    // Test simple eval
    auto result1 = co_await conn->eval("return 'hello'", std::tuple{}, std::tuple{});
    NITRO_INFO("EVAL result: %s", std::string(result1.asString()).c_str());
    NITRO_CHECK(result1.isString() && result1.asString() == "hello");

    // Test eval with keys
    auto result2 = co_await conn->eval("return KEYS[1]", std::make_tuple("mykey"), std::tuple{});
    NITRO_INFO("EVAL with key result: %s", std::string(result2.asString()).c_str());
    NITRO_CHECK(result2.isString() && result2.asString() == "mykey");

    // Test eval with keys and args
    auto result3 = co_await conn->eval(
        "return redis.call('set', KEYS[1], ARGV[1])",
        std::make_tuple("eval_key"),
        std::make_tuple("eval_value"));
    NITRO_CHECK(result3.isStatus() && result3.asString() == "OK");

    auto getResult = co_await conn->execute("GET %s", "eval_key");
    NITRO_CHECK(getResult.isString() && getResult.asString() == "eval_value");

    // Test eval with multiple keys and args
    auto result4 = co_await conn->eval(
        "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}",
        std::make_tuple("key1", "key2"),
        std::make_tuple("arg1", "arg2"));
    NITRO_CHECK(result4.isArray());

    // Cleanup
    co_await conn->execute("DEL %s", "eval_key");

    NITRO_INFO("EVAL tests passed");
    co_return;
}

NITRO_TEST(test_redis_eval_complex)
{
    NITRO_INFO("Testing complex Redis EVAL with control flow");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());

    // Setup test data
    co_await conn->execute("SET %s %s", "counter", "10");
    co_await conn->execute("SET %s %s", "threshold", "5");

    // Complex Lua script with control flow
    const char * script = R"(
        local counter = tonumber(redis.call('get', KEYS[1]))
        local threshold = tonumber(redis.call('get', KEYS[2]))
        local increment = tonumber(ARGV[1])

        if counter > threshold then
            counter = counter + increment
            redis.call('set', KEYS[1], counter)
            return {1, counter, 'incremented'}
        else
            return {0, counter, 'below threshold'}
        end
    )";

    auto result = co_await conn->eval(
        script,
        std::make_tuple("counter", "threshold"),
        std::make_tuple("3"));

    NITRO_CHECK(result.isArray());
    NITRO_INFO("Complex EVAL with control flow passed");

    // Cleanup
    co_await conn->execute("DEL %s %s", "counter", "threshold");

    co_return;
}

NITRO_TEST(test_redis_result_interface)
{
    NITRO_INFO("Testing RedisResult interface");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());

    // Test type() and copy constructor
    auto statusResult = co_await conn->execute("SET %s %s", "key1", "value1");
    RedisResult copied(statusResult);
    NITRO_CHECK(copied.type() == RedisResult::Type::Status);
    NITRO_CHECK(copied.isStatus());

    // Test copy assignment
    RedisResult assigned;
    assigned = statusResult;
    NITRO_CHECK(assigned.isStatus());

    // Test move constructor
    auto stringResult = co_await conn->execute("GET %s", "key1");
    RedisResult moved(std::move(stringResult));
    NITRO_CHECK(moved.isString());

    // Test move assignment
    RedisResult moveAssigned;
    auto tempResult = co_await conn->execute("GET %s", "key1");
    moveAssigned = std::move(tempResult);
    NITRO_CHECK(moveAssigned.isString());

    // Test isNil
    auto nilResult = co_await conn->execute("GET %s", "nonexistent_key");
    NITRO_CHECK(nilResult.isNil());
    NITRO_CHECK(nilResult.type() == RedisResult::Type::Nil);

    // Test isError
    auto errorResult = co_await conn->execute("INVALID_COMMAND");
    NITRO_CHECK(errorResult.isError());
    NITRO_CHECK(errorResult.type() == RedisResult::Type::Error);

    // Test asArray with nested elements
    co_await conn->execute("RPUSH %s %s %s %s", "list1", "a", "b", "c");
    auto arrayResult = co_await conn->execute("LRANGE %s %d %d", "list1", 0, -1);
    NITRO_CHECK(arrayResult.isArray());
    const auto & arr = arrayResult.asArray();
    NITRO_CHECK(arr.size() == 3);
    NITRO_CHECK(arr[0].isString() && arr[0].asString() == "a");
    NITRO_CHECK(arr[1].isString() && arr[1].asString() == "b");
    NITRO_CHECK(arr[2].isString() && arr[2].asString() == "c");

    // Cleanup
    co_await conn->execute("DEL %s %s", "key1", "list1");

    NITRO_INFO("RedisResult interface tests passed");
    co_return;
}

NITRO_TEST(test_redis_connection_is_alive)
{
    NITRO_INFO("Testing RedisConnection isAlive()");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());
    NITRO_REQUIRE(conn);
    NITRO_CHECK(conn->isAlive());

    // Test connection still alive after operations
    co_await conn->execute("SET %s %s", "test_key", "test_value");
    NITRO_CHECK(conn->isAlive());

    auto result = co_await conn->execute("GET %s", "test_key");
    NITRO_CHECK(conn->isAlive());
    NITRO_CHECK(result.asString() == "test_value");

    // Cleanup
    co_await conn->execute("DEL %s", "test_key");

    NITRO_INFO("RedisConnection isAlive test passed");
    co_return;
}

NITRO_TEST(test_redis_error_handling)
{
    NITRO_INFO("Testing Redis error handling");

    auto conn = co_await RedisConnection::connect(getHost(), getPort());

    // Test invalid command
    auto result = co_await conn->execute("INVALID_COMMAND");
    NITRO_CHECK(result.isError());
    NITRO_CHECK(conn->isAlive());

    // Test wrong number of arguments
    result = co_await conn->execute("SET %s", "key_only");
    NITRO_CHECK(result.isError());
    NITRO_CHECK(conn->isAlive());

    // Test type error
    co_await conn->execute("SET %s %s", "string_key", "string_value");
    result = co_await conn->execute("INCR %s", "string_key");
    NITRO_CHECK(result.isError());
    NITRO_CHECK(conn->isAlive());

    // Test invalid format specifier (should throw)
    NITRO_CHECK_THROWS_AS(co_await conn->execute("SET %z", "value"), std::runtime_error);
    NITRO_CHECK(conn->isAlive());

    co_await conn->execute("DEL %s", "string_key");

    NITRO_INFO("Redis error handling test passed");
    co_return;
}

NITRO_TEST(test_connection_interrupted)
{
    auto killer = co_await RedisConnection::connect(getHost(), getPort());

    for (int i = 0; i < 5; ++i)
    {
        auto conn = co_await RedisConnection::connect(getHost(), getPort());

        auto idResult = co_await conn->execute("CLIENT ID");
        NITRO_REQUIRE(idResult.isInteger());

        co_await killer->execute("CLIENT KILL ID %lld", idResult.asInteger());
        co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(100));

        NITRO_CHECK(!conn->isAlive());
        NITRO_CHECK_THROWS(co_await conn->execute("PING"));
        NITRO_INFO("Iteration %d passed", i + 1);
    }
}

int main(int argc, char ** argv)
{
    return nitrocoro::test::run_all(argc, argv);
}
