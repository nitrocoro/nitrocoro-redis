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
    return env ? std::stoi(env) : 6379;
}

auto factory = []() -> Task<std::unique_ptr<RedisConnection>> {
    co_return co_await RedisConnection::connect(getHost(), getPort());
};

NITRO_TEST(test_redis_pool_basic)
{
    NITRO_INFO("Testing RedisPool basic functionality");

    RedisPool pool(2, factory);
    NITRO_CHECK(pool.idleCount() == 0); // Pool starts empty

    // Test acquire and basic usage
    auto conn = co_await pool.acquire();
    NITRO_REQUIRE(conn);
    NITRO_CHECK(conn->isAlive());

    // Test connection works
    auto result = co_await conn->execute("PING");
    NITRO_CHECK(!result.isError());
    NITRO_CHECK(result.asString() == "PONG");

    NITRO_INFO("RedisPool basic test passed");
}

NITRO_TEST(test_pooled_connection_auto_return)
{
    NITRO_INFO("Testing PooledConnection auto return");

    RedisPool pool(1, factory);

    {
        auto conn = co_await pool.acquire();
        NITRO_REQUIRE(conn);
        NITRO_CHECK(conn->isAlive());
        NITRO_CHECK(pool.idleCount() == 0);
        // conn destroyed here, auto returned
    }

    // Wait for async return
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(pool.idleCount() == 1);

    NITRO_INFO("PooledConnection auto return test passed");
}


NITRO_TEST(test_redis_pool_max_size)
{
    NITRO_INFO("Testing RedisPool max size and blocking");

    RedisPool pool(1, factory);

    auto conn1 = co_await pool.acquire();
    NITRO_REQUIRE(conn1);

    // Test blocking behavior
    bool acquired = false;
    Promise<> acquiredPromise;
    Scheduler::current()->spawn([&]() -> Task<> {
        auto conn2 = co_await pool.acquire();
        acquired = true;
        acquiredPromise.set_value();
    });

    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(!acquired);

    // Release and verify unblocking
    conn1.reset();
    co_await acquiredPromise.get_future().get();
    NITRO_CHECK(acquired);
    NITRO_INFO("RedisPool max size test passed");
}

NITRO_TEST(test_redis_pool_factory_failure)
{
    NITRO_INFO("Testing RedisPool factory failure handling");

    auto failingFactory = []() -> Task<std::unique_ptr<RedisConnection>> {
        throw std::runtime_error("Connection failed");
        co_return nullptr; // unreachable
    };

    RedisPool pool(1, failingFactory);

    // Test that acquire() throws the expected exception
    NITRO_CHECK_THROWS_AS(co_await pool.acquire(), std::runtime_error);
    NITRO_CHECK(pool.idleCount() == 0);

    NITRO_INFO("RedisPool factory failure test passed");
}

NITRO_TEST(test_pooled_connection_is_alive)
{
    NITRO_INFO("Testing PooledConnection isAlive()");

    RedisPool pool(1, factory);

    auto conn = co_await pool.acquire();
    NITRO_REQUIRE(conn);
    NITRO_CHECK(conn->isAlive());

    // Test connection still alive after operations
    auto result = co_await conn->execute("SET %s %s", "alive_key", "alive_value");
    NITRO_CHECK(conn->isAlive());

    result = co_await conn->execute("GET %s", "alive_key");
    NITRO_CHECK(conn->isAlive());
    NITRO_CHECK(result.asString() == "alive_value");

    // Cleanup
    co_await conn->execute("DEL %s", "alive_key");

    NITRO_INFO("PooledConnection isAlive test passed");
}

int main()
{
    return nitrocoro::test::run_all();
}
