#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/testing/Test.h>

using namespace nitrocoro;
using namespace nitrocoro::redis;

NITRO_TEST(test_redis_client)
{
    NITRO_INFO("Testing RedisClient\n");

    RedisConnection conn("127.0.0.1", 6379);
    co_await conn.connect();
    NITRO_INFO("Connected to Redis\n");

    // Test SET
    auto setResult = co_await conn.execute("SET %s %s", "test_key", "test_value");
    NITRO_INFO("SET result: %s\n", std::string(setResult.asString()).c_str());
    NITRO_CHECK(setResult.isStatus() && setResult.asString() == "OK");

    // Test GET
    auto getResult = co_await conn.execute("GET %s", "test_key");
    NITRO_INFO("GET result: %s\n", std::string(getResult.asString()).c_str());
    NITRO_CHECK(getResult.isString() && getResult.asString() == "test_value");

    // Test INCR
    auto incrResult = co_await conn.execute("INCR %s", "counter");
    NITRO_INFO("INCR result: %lld\n", incrResult.asInteger());
    NITRO_CHECK(incrResult.isInteger());

    // Test DEL
    auto delResult = co_await conn.execute("DEL %s %s", "test_key", "counter");
    NITRO_INFO("DEL result: %lld\n", delResult.asInteger());
    NITRO_CHECK(delResult.isInteger() && delResult.asInteger() == 2);

    // Test disconnect
    co_await conn.disconnect();
    NITRO_INFO("Disconnected from Redis\n");

    NITRO_INFO("All tests passed\n");
    co_return;
}

NITRO_TEST(test_redis_auto_disconnect)
{
    NITRO_INFO("Testing auto disconnect\n");

    {
        RedisConnection conn("127.0.0.1", 6379);
        co_await conn.connect();
        NITRO_INFO("Connected to Redis\n");

        auto setResult = co_await conn.execute("SET %s %s", "auto_key", "auto_value");
        NITRO_CHECK(setResult.isStatus() && setResult.asString() == "OK");

        // conn will be destroyed here, triggering auto disconnect
    }

    NITRO_INFO("Auto disconnect test passed\n");
    co_return;
}

NITRO_TEST(test_redis_eval)
{
    NITRO_INFO("Testing Redis EVAL\n");

    RedisConnection conn("127.0.0.1", 6379);
    co_await conn.connect();

    // Test simple eval
    auto result1 = co_await conn.eval("return 'hello'", std::tuple{}, std::tuple{});
    NITRO_INFO("EVAL result: %s\n", std::string(result1.asString()).c_str());
    NITRO_CHECK(result1.isString() && result1.asString() == "hello");

    // Test eval with keys
    auto result2 = co_await conn.eval("return KEYS[1]", std::make_tuple("mykey"), std::tuple{});
    NITRO_INFO("EVAL with key result: %s\n", std::string(result2.asString()).c_str());
    NITRO_CHECK(result2.isString() && result2.asString() == "mykey");

    // Test eval with keys and args
    auto result3 = co_await conn.eval(
        "return redis.call('set', KEYS[1], ARGV[1])",
        std::make_tuple("eval_key"),
        std::make_tuple("eval_value"));
    NITRO_CHECK(result3.isStatus() && result3.asString() == "OK");

    auto getResult = co_await conn.execute("GET %s", "eval_key");
    NITRO_CHECK(getResult.isString() && getResult.asString() == "eval_value");

    // Test eval with multiple keys and args
    auto result4 = co_await conn.eval(
        "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}",
        std::make_tuple("key1", "key2"),
        std::make_tuple("arg1", "arg2"));
    NITRO_CHECK(result4.isArray());

    // Cleanup
    co_await conn.execute("DEL %s", "eval_key");
    co_await conn.disconnect();

    NITRO_INFO("EVAL tests passed\n");
    co_return;
}

int main()
{
    return nitrocoro::test::run_all();
}
