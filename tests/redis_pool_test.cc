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

auto factory = []() -> Task<std::unique_ptr<RedisConnection>> {
    auto conn = std::make_unique<RedisConnection>(getHost(), getPort());
    co_await conn->connect();
    co_return conn;
};

NITRO_TEST(test_redis_pool_basic)
{
    NITRO_INFO("Testing RedisPool basic functionality\n");

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
    NITRO_CHECK(!result2.isError());

    NITRO_INFO("RedisPool multiple connections test passed\n");
}

NITRO_TEST(test_redis_pool_auto_return)
{
    NITRO_INFO("Testing RedisPool automatic connection return\n");

    RedisPool pool(1, factory);

    // Acquire and release connection in scope
    {
        auto conn = co_await pool.acquire();
        NITRO_REQUIRE(conn);
        NITRO_CHECK(pool.idleCount() == 0);
    } // Connection should auto-return here

    // Wait for async return to complete
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));

    // Verify connection was returned
    NITRO_CHECK(pool.idleCount() == 1);

    // Acquire again should reuse the connection
    auto conn2 = co_await pool.acquire();
    NITRO_REQUIRE(conn2);
    NITRO_CHECK(pool.idleCount() == 0);

    NITRO_INFO("RedisPool auto-return test passed\n");
}

NITRO_TEST(test_redis_pool_max_size_blocking)
{
    NITRO_INFO("Testing RedisPool max size and blocking\n");

    RedisPool pool(1, factory);

    auto conn1 = co_await pool.acquire();
    NITRO_REQUIRE(conn1);

    // Start acquiring second connection (should block)
    bool acquired = false;
    Scheduler::current()->spawn([&]() -> Task<> {
        auto conn2 = co_await pool.acquire();
        acquired = true;
        co_return;
    });

    // Give some time, should still be blocked
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(!acquired);

    // Release first connection
    conn1.reset();

    // Now second acquire should complete
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(acquired);

    NITRO_INFO("RedisPool blocking test passed\n");
}

NITRO_TEST(test_redis_pool_shared_ptr_conversion)
{
    NITRO_INFO("Testing RedisPool shared_ptr conversion\n");

    RedisPool pool(1, factory);

    std::shared_ptr<RedisConnection> sharedConn;
    {
        auto conn = co_await pool.acquire();
        NITRO_REQUIRE(conn);

        // Convert to shared_ptr
        sharedConn = std::move(conn);
        NITRO_REQUIRE(sharedConn);
        NITRO_CHECK(pool.idleCount() == 0);
    }

    // Connection should not be returned yet (shared_ptr still holds it)
    NITRO_CHECK(pool.idleCount() == 0);

    // Release shared_ptr
    sharedConn.reset();

    // Wait for async return to complete
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));

    // Now connection should be returned
    NITRO_CHECK(pool.idleCount() == 1);

    NITRO_INFO("RedisPool shared_ptr conversion test passed\n");
}

NITRO_TEST(test_redis_pool_factory_failure)
{
    NITRO_INFO("Testing RedisPool factory failure handling\n");

    auto failingFactory = []() -> Task<std::unique_ptr<RedisConnection>> {
        throw std::runtime_error("Connection failed");
    };

    RedisPool pool(1, failingFactory);

    bool exceptionCaught = false;
    try
    {
        auto conn = co_await pool.acquire();
    }
    catch (const std::runtime_error &)
    {
        exceptionCaught = true;
    }

    NITRO_CHECK(exceptionCaught);
    NITRO_CHECK(pool.idleCount() == 0);

    NITRO_INFO("RedisPool factory failure test passed\n");
}

NITRO_TEST(test_redis_pool_lifecycle_safety)
{
    NITRO_INFO("Testing RedisPool lifecycle safety\n");

    std::unique_ptr<RedisConnection> survivingConn;

    {
        RedisPool pool(1, factory);
        auto conn = co_await pool.acquire();
        NITRO_REQUIRE(conn);

        // Extract raw connection (simulating pool destruction)
        survivingConn = std::unique_ptr<RedisConnection>(conn.release());
    } // Pool destroyed here

    // Connection should still be valid
    NITRO_REQUIRE(survivingConn);
    auto result = co_await survivingConn->execute("PING");
    NITRO_CHECK(!result.isError());

    NITRO_INFO("RedisPool lifecycle safety test passed\n");
}

int main()
{
    return nitrocoro::test::run_all();
}
