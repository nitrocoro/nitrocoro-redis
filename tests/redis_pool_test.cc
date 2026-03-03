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

auto factory = []() -> Task<RedisConnection> {
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

    // Test connection works
    auto result = co_await conn->execute("PING");
    NITRO_CHECK(!result.isError());
    NITRO_CHECK(result.asString() == "PONG");

    NITRO_INFO("RedisPool basic test passed");
}

NITRO_TEST(test_pooled_connection_reset)
{
    NITRO_INFO("Testing PooledConnection reset()");

    RedisPool pool(1, factory);

    auto conn = co_await pool.acquire();
    NITRO_REQUIRE(conn);
    NITRO_CHECK(pool.idleCount() == 0);

    // Test reset()
    conn.reset();
    NITRO_CHECK(!conn);

    // Wait for async return
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(pool.idleCount() == 1);

    NITRO_INFO("PooledConnection reset test passed");
}

NITRO_TEST(test_pooled_connection_detach)
{
    NITRO_INFO("Testing PooledConnection detach()");

    RedisPool pool(1, factory);

    auto conn = co_await pool.acquire();
    NITRO_REQUIRE(conn);

    // Test detach() returns value, not pointer
    RedisConnection rawConn = conn.detach();
    NITRO_REQUIRE(rawConn);
    NITRO_CHECK(!conn);

    // Connection should still work
    auto result = co_await rawConn.execute("PING");
    NITRO_CHECK(!result.isError());

    // Test that detached connection can be moved
    RedisConnection movedConn = std::move(rawConn);
    NITRO_CHECK(!rawConn); // moved-from should be empty
    NITRO_REQUIRE(movedConn);

    // Wait and verify connection not returned to pool
    co_await Scheduler::current()->sleep_for(std::chrono::milliseconds(10));
    NITRO_CHECK(pool.idleCount() == 0);

    NITRO_INFO("PooledConnection detach test passed");
}

NITRO_TEST(test_pooled_connection_move)
{
    NITRO_INFO("Testing PooledConnection move semantics");

    RedisPool pool(1, factory);

    auto conn1 = co_await pool.acquire();
    NITRO_REQUIRE(conn1);

    // Test move constructor
    auto conn2 = std::move(conn1);
    NITRO_CHECK(!conn1);
    NITRO_REQUIRE(conn2);

    // Test move assignment
    PooledConnection conn3;
    NITRO_CHECK(!conn3);
    conn3 = std::move(conn2);
    NITRO_CHECK(!conn2);
    NITRO_REQUIRE(conn3);

    NITRO_INFO("PooledConnection move test passed");
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

    auto failingFactory = []() -> Task<RedisConnection> {
        throw std::runtime_error("Connection failed");
        co_return RedisConnection{}; // unreachable
    };

    RedisPool pool(1, failingFactory);

    // Test that acquire() throws the expected exception
    NITRO_CHECK_THROWS_AS(co_await pool.acquire(), std::runtime_error);
    NITRO_CHECK(pool.idleCount() == 0);

    NITRO_INFO("RedisPool factory failure test passed");
}

NITRO_TEST(test_pooled_connection_empty_detach)
{
    NITRO_INFO("Testing PooledConnection detach() on empty connection");

    // Test detach on default constructed PooledConnection
    PooledConnection empty;
    NITRO_CHECK(!empty);
    NITRO_CHECK_THROWS_AS(empty.detach(), std::runtime_error);

    NITRO_INFO("PooledConnection empty detach test passed");

    co_return;
}

int main()
{
    return nitrocoro::test::run_all();
}
