// shm_bench.cpp — benchmark for the LCM "ipcshm://" provider.
//
// Measures:
//   lat  : round-trip (ping-pong) latency.  The publisher sends one message
//          at a time and waits for the subscriber's echo.
//   thr  : one-way throughput + one-way latency.  The publisher blasts
//          `count` messages; the subscriber measures receive rate and
//          (same-host) CLOCK_MONOTONIC latency.
//
// Usage:
//   shm_bench lat pub <payload_size> <count>
//   shm_bench lat sub <payload_size> <count>
//   shm_bench thr pub <payload_size> <count>
//   shm_bench thr sub <payload_size> <count>
//
// Run the subscriber first, then the publisher (either order works: the
// subscriber re-announces readiness until the first data message arrives).
//
// Example (large 1 MiB payloads, 100 round trips):
//   # on the board
//   ./shm_bench lat sub 1048576 100 &
//   ./shm_bench lat pub 1048576 100

#include <lcm/lcm.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <algorithm>
#include <vector>

#define DATA_CHANNEL "bench/data"
#define ECHO_CHANNEL "bench/echo"
#define CTRL_CHANNEL "bench/ctrl"

#define HDR_MAGIC 0x53484d42454e4348ULL  // "SHMBENCH"

// Every data payload starts with this 24-byte header.
struct bench_hdr {
    uint64_t magic;
    uint64_t seq;
    uint64_t send_ts_ns;  // CLOCK_MONOTONIC nanoseconds at publish time
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void hdr_set(uint8_t *p, uint64_t seq, uint64_t ts)
{
    uint64_t magic = HDR_MAGIC;
    memcpy(p + 0, &magic, 8);
    memcpy(p + 8, &seq, 8);
    memcpy(p + 16, &ts, 8);
}

static uint64_t hdr_seq(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p + 8, 8);
    return v;
}

static uint64_t hdr_ts(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p + 16, 8);
    return v;
}

static bool hdr_ok(const uint8_t *p, uint32_t size)
{
    uint64_t v;
    if (size < sizeof(bench_hdr))
        return false;
    memcpy(&v, p, 8);
    return v == HDR_MAGIC;
}

static void fill_payload(std::vector<uint8_t> &buf, size_t size, uint64_t seq, uint64_t ts)
{
    buf.resize(size);
    memset(&buf[0], 0, size);
    hdr_set(&buf[0], seq, ts);
    for (size_t i = sizeof(bench_hdr); i < size; i++)
        buf[i] = (uint8_t)(i & 0xff);
}

// ---------------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------------
static void print_stats(std::vector<uint64_t> &v, const char *label)
{
    if (v.empty()) {
        printf("%-10s : no samples\n", label);
        return;
    }
    std::sort(v.begin(), v.end());
    uint64_t n = v.size();
    uint64_t sum = 0;
    for (size_t i = 0; i < n; i++)
        sum += v[i];

    uint64_t p50 = v[n * 50 / 100];
    uint64_t p95 = v[n * 95 / 100];
    uint64_t p99 = v[n * 99 / 100];

    printf("%-10s : n=%" PRIu64 "  min=%" PRIu64 "  avg=%" PRIu64 "  p50=%" PRIu64
           "  p95=%" PRIu64 "  p99=%" PRIu64 "  max=%" PRIu64 "  (ns)\n",
           label, n, v[0], sum / n, p50, p95, p99, v[n - 1]);
    printf("             %.2f us / %.2f us / %.2f us / %.2f us / %.2f us / %.2f us\n",
           v[0] / 1000.0, (sum / n) / 1000.0, p50 / 1000.0, p95 / 1000.0,
           p99 / 1000.0, v[n - 1] / 1000.0);
}

// ---------------------------------------------------------------------------
// shared: ready handshake (sub re-announces, so start order does not matter)
// ---------------------------------------------------------------------------
struct CtlCtx {
    bool ready;
};

static void ctl_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user)
{
    (void)rbuf;
    (void)channel;
    ((CtlCtx *)user)->ready = true;
}

static void wait_ready(lcm_t *lcm, CtlCtx *ctl)
{
    uint64_t deadline = now_ns() + 30ULL * 1000000000ULL;  // 30 s
    while (!ctl->ready) {
        if (now_ns() > deadline) {
            fprintf(stderr, "ERROR: timed out waiting for subscriber\n");
            exit(1);
        }
        lcm_handle_timeout(lcm, 200);
    }
}

// ---------------------------------------------------------------------------
// latency mode
// ---------------------------------------------------------------------------
struct LatEchoCtx {
    lcm_t *lcm;
    uint64_t echoed;
};

static void lat_sub_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user)
{
    (void)channel;
    LatEchoCtx *c = (LatEchoCtx *)user;
    if (hdr_ok((const uint8_t *)rbuf->data, rbuf->data_size)) {
        lcm_publish(c->lcm, ECHO_CHANNEL, rbuf->data, rbuf->data_size);
        c->echoed++;
    }
}

struct LatPubCtx {
    bool got;
    uint64_t seq;
};

static void lat_pub_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user)
{
    (void)channel;
    LatPubCtx *c = (LatPubCtx *)user;
    if (hdr_ok((const uint8_t *)rbuf->data, rbuf->data_size)) {
        c->seq = hdr_seq((const uint8_t *)rbuf->data);
        c->got = true;
    }
}

static void run_lat_pub(lcm_t *lcm, size_t size, uint64_t count)
{
    CtlCtx ctl = {false};
    LatPubCtx pub = {false, 0};
    lcm_subscribe(lcm, CTRL_CHANNEL, ctl_handler, &ctl);
    lcm_subscribe(lcm, ECHO_CHANNEL, lat_pub_handler, &pub);

    printf("lat pub: waiting for subscriber...\n");
    wait_ready(lcm, &ctl);

    std::vector<uint8_t> buf;
    std::vector<uint64_t> rtts;
    rtts.reserve(count);

    for (uint64_t seq = 1; seq <= count; seq++) {
        fill_payload(buf, size, seq, 0);
        pub.got = false;
        uint64_t t0 = now_ns();
        lcm_publish(lcm, DATA_CHANNEL, &buf[0], size);

        uint64_t deadline = t0 + 10ULL * 1000000000ULL;  // 10 s watchdog
        while (!pub.got) {
            if (now_ns() > deadline) {
                fprintf(stderr, "lat pub: no echo for seq %" PRIu64 ", aborting\n", seq);
                return;
            }
            lcm_handle_timeout(lcm, 1000);
        }
        rtts.push_back(now_ns() - t0);
    }

    print_stats(rtts, "lat RTT");
}

static void run_lat_sub(lcm_t *lcm, size_t size, uint64_t count)
{
    (void)size;
    LatEchoCtx ctx = {lcm, 0};
    lcm_subscribe(lcm, DATA_CHANNEL, lat_sub_handler, &ctx);

    printf("lat sub: ready (%" PRIu64 " echoes expected)\n", count);
    uint64_t next_ready = 0;
    while (ctx.echoed < count) {
        if (now_ns() >= next_ready) {
            uint8_t r = 'R';
            lcm_publish(lcm, CTRL_CHANNEL, &r, 1);
            next_ready = now_ns() + 200ULL * 1000000ULL;  // re-announce every 200 ms
        }
        lcm_handle_timeout(lcm, 100);
    }
    printf("lat sub: echoed %" PRIu64 " messages\n", ctx.echoed);
}

// ---------------------------------------------------------------------------
// throughput mode
// ---------------------------------------------------------------------------
struct ThrSubCtx {
    uint64_t received;
    std::vector<uint64_t> lats;
};

static void thr_sub_handler(const lcm_recv_buf_t *rbuf, const char *channel, void *user)
{
    (void)channel;
    ThrSubCtx *c = (ThrSubCtx *)user;
    if (hdr_ok((const uint8_t *)rbuf->data, rbuf->data_size)) {
        c->lats.push_back(now_ns() - hdr_ts((const uint8_t *)rbuf->data));
        c->received++;
    }
}

static void run_thr_pub(lcm_t *lcm, size_t size, uint64_t count)
{
    CtlCtx ctl = {false};
    lcm_subscribe(lcm, CTRL_CHANNEL, ctl_handler, &ctl);

    printf("thr pub: waiting for subscriber...\n");
    wait_ready(lcm, &ctl);

    std::vector<uint8_t> buf;
    uint64_t t0 = now_ns();
    for (uint64_t seq = 1; seq <= count; seq++) {
        fill_payload(buf, size, seq, now_ns());
        lcm_publish(lcm, DATA_CHANNEL, &buf[0], size);
    }
    uint64_t t1 = now_ns();

    double secs = (t1 - t0) / 1e9;
    printf("thr pub: sent %" PRIu64 " x %zu B in %.3f s  = %.1f msg/s, %.2f MB/s\n",
           count, size, secs, count / secs, (count * size) / secs / 1e6);
}

static void run_thr_sub(lcm_t *lcm, size_t size, uint64_t count)
{
    (void)size;
    ThrSubCtx ctx = {0, {}};
    ctx.lats.reserve(count);
    lcm_subscribe(lcm, DATA_CHANNEL, thr_sub_handler, &ctx);

    printf("thr sub: ready (expecting %" PRIu64 " messages)\n", count);
    uint64_t next_ready = 0;
    uint64_t t0 = now_ns();
    while (ctx.received < count) {
        if (now_ns() >= next_ready) {
            uint8_t r = 'R';
            lcm_publish(lcm, CTRL_CHANNEL, &r, 1);
            next_ready = now_ns() + 200ULL * 1000000ULL;
        }
        lcm_handle_timeout(lcm, 100);

        // Guard against a publisher that died mid-test.
        if (ctx.received == 0 && now_ns() - t0 > 30ULL * 1000000000ULL) {
            fprintf(stderr, "thr sub: no data within 30 s, aborting\n");
            break;
        }
    }
    uint64_t t1 = now_ns();

    if (ctx.received > 0) {
        double secs = (t1 - t0) / 1e9;
        printf("thr sub: received %" PRIu64 " msgs in %.3f s = %.1f msg/s, %.2f MB/s\n",
               ctx.received, secs, ctx.received / secs, (ctx.received * size) / secs / 1e6);
        print_stats(ctx.lats, "thr 1-way");
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s lat pub <payload_size> <count>\n"
            "  %s lat sub <payload_size> <count>\n"
            "  %s thr pub <payload_size> <count>\n"
            "  %s thr sub <payload_size> <count>\n",
            prog, prog, prog, prog);
}

int main(int argc, char **argv)
{
    if (argc != 5) {
        usage(argv[0]);
        return 1;
    }

    const char *mode = argv[1];
    const char *role = argv[2];
    size_t size = strtoul(argv[3], NULL, 10);
    uint64_t count = strtoull(argv[4], NULL, 10);

    if (size < sizeof(bench_hdr))
        size = sizeof(bench_hdr);
    if (count == 0)
        count = 1;

    lcm_t *lcm = lcm_create("ipcshm://");
    if (!lcm) {
        fprintf(stderr, "lcm_create(\"ipcshm://\") failed\n");
        return 1;
    }

    printf("shm_bench: mode=%s role=%s payload=%zu B count=%" PRIu64 "\n",
           mode, role, size, count);

    if (strcmp(mode, "lat") == 0 && strcmp(role, "pub") == 0)
        run_lat_pub(lcm, size, count);
    else if (strcmp(mode, "lat") == 0 && strcmp(role, "sub") == 0)
        run_lat_sub(lcm, size, count);
    else if (strcmp(mode, "thr") == 0 && strcmp(role, "pub") == 0)
        run_thr_pub(lcm, size, count);
    else if (strcmp(mode, "thr") == 0 && strcmp(role, "sub") == 0)
        run_thr_sub(lcm, size, count);
    else {
        usage(argv[0]);
        lcm_destroy(lcm);
        return 1;
    }

    lcm_destroy(lcm);
    return 0;
}
