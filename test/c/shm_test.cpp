#include <gtest/gtest.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <lcm/lcm.h>

// ========== helper: clean shared state before/after tests ==========
static void cleanup_shm()
{
    shm_unlink("/ipcshm000");
}

// ========== basic construct / destroy ==========
TEST(LCM_SHMC, ConstructDestroy)
{
    lcm_t *lcm = lcm_create("ipcshm://");
    EXPECT_TRUE(lcm != NULL);
    lcm_destroy(lcm);
}

TEST(LCM_SHMC, DoubleConstruct)
{
    lcm_t *lcm1 = lcm_create("ipcshm://");
    EXPECT_TRUE(lcm1 != NULL);
    // second instance in the same process — should still work
    lcm_t *lcm2 = lcm_create("ipcshm://");
    EXPECT_TRUE(lcm2 != NULL);
    lcm_destroy(lcm2);
    lcm_destroy(lcm1);
}

// ========== simple message round-trip ==========
struct ShmSimpleState {
    std::vector<uint8_t> buf;
};

static void shm_simple_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user_data)
{
    (void)channel;
    ShmSimpleState *s = (ShmSimpleState *)user_data;
    s->buf.resize(rbuf->data_size);
    memcpy(&s->buf[0], rbuf->data, rbuf->data_size);
}

TEST(LCM_SHMC, SimpleRoundTrip)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    ShmSimpleState state;
    lcm_subscribe(lcm, "test_channel", shm_simple_handler, &state);

    const int buf_size = 500;
    std::vector<uint8_t> send_buf(buf_size);
    for (int i = 0; i < buf_size; i++)
        send_buf[i] = (uint8_t)(rand() % 256);

    lcm_publish(lcm, "test_channel", &send_buf[0], buf_size);
    lcm_handle(lcm);

    EXPECT_EQ(send_buf, state.buf);

    lcm_destroy(lcm);
}

// ========== large message (> 1024, triggers msgr->buff malloc) ==========
TEST(LCM_SHMC, LargeMessage)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    ShmSimpleState state;
    lcm_subscribe(lcm, "large_channel", shm_simple_handler, &state);

    // 4096 bytes — exceeds shm_msg_t::data[1024], triggers heap allocation path
    const int buf_size = 4096;
    std::vector<uint8_t> send_buf(buf_size);
    for (int i = 0; i < buf_size; i++)
        send_buf[i] = (uint8_t)(rand() % 256);

    lcm_publish(lcm, "large_channel", &send_buf[0], buf_size);
    lcm_handle(lcm);

    EXPECT_EQ(send_buf, state.buf);

    lcm_destroy(lcm);
}

// ========== multiple messages in a row (tests handle re-arm) ==========
struct ShmBufferedState {
    std::vector<std::vector<uint8_t> > received;
};

static void shm_buffered_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user_data)
{
    (void)channel;
    ShmBufferedState *s = (ShmBufferedState *)user_data;
    std::vector<uint8_t> buf(rbuf->data_size);
    memcpy(&buf[0], rbuf->data, rbuf->data_size);
    s->received.push_back(buf);
}

TEST(LCM_SHMC, BufferedMessages)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    ShmBufferedState state;
    lcm_subscribe(lcm, "buf_channel", shm_buffered_handler, &state);

    const int num_msgs = 20;
    const int buf_size = 256;
    std::vector<std::vector<uint8_t> > send_bufs(num_msgs);
    for (int n = 0; n < num_msgs; n++) {
        send_bufs[n].resize(buf_size);
        for (int i = 0; i < buf_size; i++)
            send_bufs[n][i] = (uint8_t)((n << 4) | (i & 0x0f));
        lcm_publish(lcm, "buf_channel", &send_bufs[n][0], buf_size);
    }

    // Handle all messages — one per handle() call with FIFO re-arm between calls
    for (int n = 0; n < num_msgs; n++) {
        lcm_handle(lcm);
    }

    EXPECT_EQ((int)state.received.size(), num_msgs);
    for (int n = 0; n < num_msgs; n++) {
        EXPECT_EQ(send_bufs[n], state.received[n]);
    }

    lcm_destroy(lcm);
}

// ========== no subscribers — message dropped gracefully ==========
TEST(LCM_SHMC, NoSubscribers)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    uint8_t data[16] = {0};
    // publish without any subscriber should not crash
    int ret = lcm_publish(lcm, "no_sub_channel", data, sizeof(data));
    EXPECT_GE(ret, 0);
    lcm_handle(lcm);

    lcm_destroy(lcm);
}

// ========== unsubscribe ==========
TEST(LCM_SHMC, Unsubscribe)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    ShmSimpleState state;
    lcm_subscription_t *h = lcm_subscribe(lcm, "unsub_channel", shm_simple_handler, &state);
    ASSERT_TRUE(h != NULL);

    uint8_t data[16] = {42};
    lcm_publish(lcm, "unsub_channel", data, sizeof(data));
    lcm_handle(lcm);
    EXPECT_FALSE(state.buf.empty());

    // Unsubscribe and publish again — handler should NOT be called
    state.buf.clear();
    lcm_unsubscribe(lcm, h);

    uint8_t data2[16] = {99};
    lcm_publish(lcm, "unsub_channel", data2, sizeof(data2));
    lcm_handle(lcm);
    EXPECT_TRUE(state.buf.empty());

    lcm_destroy(lcm);
}

// ========== handle_timeout (no fd — falls back to blocking handle) ==========
TEST(LCM_SHMC, HandleTimeout)
{
    cleanup_shm();

    lcm_t *lcm = lcm_create("ipcshm://");
    ASSERT_TRUE(lcm != NULL);

    // SHM has no fd, so lcm_handle_timeout falls back to lcm_handle().
    ShmSimpleState state;
    lcm_subscribe(lcm, "timeout_ch", shm_simple_handler, &state);
    lcm_publish(lcm, "timeout_ch", "hello", 5);
    int ret = lcm_handle_timeout(lcm, 5000);
    EXPECT_GE(ret, 0);  // no fd: falls back to lcm_handle, returns 0
    EXPECT_FALSE(state.buf.empty());  // message was actually delivered

    lcm_destroy(lcm);
}

int main(int argc, char **argv)
{
    cleanup_shm();
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    cleanup_shm();
    return ret;
}
