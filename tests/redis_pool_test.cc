#include <nitrocoro/redis/RedisPool.h>
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
    return env ? std::atoi(env) : 6379;
}

NITRO_TEST(test_redis_pool_basic)
{
    NITRO_INFO("Testing RedisPool basic functionality\n");

    auto factory = []() -> Task<std::shared_ptr<RedisConnection>> {
        auto conn = std::make_shared<RedisConnection>(getHost(), getPort());
        co_await conn->connect();
        co_return conn;
    };

    RedisPool pool(2, factory);

    // Test acquire connection
    auto conn = co_await pool.acquire();
    NITRO_REQUIRE(conn);

    // Test connection works
    auto result = co_await conn->execute("PING");
    NITRO_CHECK(!result.isError());
    NITRO_CHECK(result.asString() == "PONG");

    NITRO_INFO("RedisPool basic test passed\n");
}

NITRO_TEST(test_redis_pool_multiple_connections)
{
    NITRO_INFO("Testing RedisPool multiple connections\n");

    auto factory = []() -> Task<std::shared_ptr<RedisConnection>> {
        auto conn = std::make_shared<RedisConnection>(getHost(), getPort());
        co_await conn->connect();
        co_return conn;
    };

    RedisPool pool(2, factory);

    // Acquire two connections
    auto conn1 = co_await pool.acquire();
    auto conn2 = co_await pool.acquire();

    NITRO_REQUIRE(conn1);
    NITRO_REQUIRE(conn2);

    // Test both work independently
    auto result1 = co_await conn1->execute("SET %s %s", "key1", "value1");
    auto result2 = co_await conn2->execute("SET %s %s", "key2", "value2");

    NITRO_CHECK(!result1.isError());
    NITRO_CHECK(!result1.isError());

    NITRO_INFO("RedisPool multiple connections test passed\n");
}

int main()
{
    return nitrocoro::test::run_all();
}
