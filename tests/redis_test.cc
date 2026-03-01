#include <nitrocoro/redis/RedisClient.h>
#include <nitrocoro/testing/Test.h>

using namespace nitrocoro;
using namespace nitrocoro::redis;

NITRO_TEST(test_redis_client)
{
    NITRO_INFO("Testing RedisClient");

    RedisClient client("127.0.0.1", 6379);
    co_await client.connect();

    co_await client.set("test_key", "test_value");
    auto value = co_await client.get("test_key");
    NITRO_CHECK_EQ(value, "test_value");

    co_await client.del("test_key");
    auto deleted = co_await client.get("test_key");
    NITRO_CHECK_EQ(deleted, "");

    co_return;
}

int main()
{
    return nitrocoro::test::run_all();
}
