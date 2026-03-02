#include <nitrocoro/redis/RedisConnection.h>
#include <nitrocoro/testing/Test.h>

using namespace nitrocoro;
using namespace nitrocoro::redis;

NITRO_TEST(test_redis_client)
{
    NITRO_INFO("Testing RedisClient\n");

    RedisConnection conn("127.0.0.1", 6379);
    co_await conn.connect();

    NITRO_INFO("connected\n");
    NITRO_CHECK(true);

    co_return;
}

int main()
{
    return nitrocoro::test::run_all();
}
