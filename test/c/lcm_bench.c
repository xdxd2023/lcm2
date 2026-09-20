/* lcm_bench.c — ipcshm/udpm 延迟、CPU 与通道速率基准 (设备可用, 纯 C)
 *
 *   lcm_bench sub     <url> <count> [glib|loop]   订阅并统计 发布->回调 延迟
 *   lcm_bench pub     <url> <count> <rate_hz>     发布带时间戳消息, 末尾发 done
 *   lcm_bench cpu     <url> <seconds>             只发布(不 handle), 自报 CPU
 *   lcm_bench poll    <url> <period_ms> <seconds> 只轮询 handle_timeout, 自报 CPU
 *   lcm_bench monitor <url> <seconds> [chan_re]   统计各通道消息率, 用来确认谁在发
 *
 * 先起 sub(后台), 再起 pub。sub 的 glib 模式复刻 lcm_tunnel/src/lcm_util.c 的挂载
 * 逻辑: 有 fd 走 G_IO_IN, 没有 fd 退回 10ms 定时器 —— 这正是 ipcshm 旧版的问题所在。
 */
#include <lcm/lcm.h>
#include <glib.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#define CH_DATA "bench/lat"
#define CH_DONE "bench/done"
typedef struct { uint64_t seq; uint64_t ts_ns; } hdr_t;

static uint64_t now_ns(void)
{ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec; }
static double cpu_s(void)
{ struct rusage ru; getrusage(RUSAGE_SELF, &ru);
  return ru.ru_utime.tv_sec + ru.ru_utime.tv_usec * 1e-6
       + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec * 1e-6; }

static uint64_t *g_lats; static size_t g_n, g_cap; static int g_printed;
static uint64_t g_recv;
static void push_lat(uint64_t v)
{ if (g_n == g_cap) { g_cap = g_cap ? g_cap * 2 : 1024;
      g_lats = (uint64_t *) realloc(g_lats, g_cap * sizeof(uint64_t)); }
  g_lats[g_n++] = v; }
static int cmp_u64(const void *a, const void *b)
{ uint64_t x = *(const uint64_t *) a, y = *(const uint64_t *) b;
  return (x > y) - (x < y); }
static void print_lat(const char *tag)
{ g_printed = 1;
  if (g_n == 0) { printf("%s: no samples\n", tag); return; }
  qsort(g_lats, g_n, sizeof(uint64_t), cmp_u64);
  double sum = 0; for (size_t i = 0; i < g_n; i++) sum += g_lats[i];
  printf("%s: n=%zu min=%.1f avg=%.1f p50=%.1f p90=%.1f p99=%.1f max=%.1f (us)\n",
         tag, g_n, g_lats[0]/1e3, sum/g_n/1e3, g_lats[g_n*50/100]/1e3,
         g_lats[g_n*90/100]/1e3, g_lats[g_n*99/100]/1e3, g_lats[g_n-1]/1e3);
  fflush(stdout); }

typedef struct { char name[64]; uint64_t cnt; } chcnt_t;
static chcnt_t g_ch[32]; static int g_nch;
static void on_mon(const lcm_recv_buf_t *rbuf, const char *ch, void *user)
{ (void) rbuf; (void) user;
  __atomic_add_fetch(&g_recv, 1, __ATOMIC_RELAXED);
  for (int i = 0; i < g_nch; i++) if (!strcmp(g_ch[i].name, ch)) { g_ch[i].cnt++; return; }
  if (g_nch < 32) { strncpy(g_ch[g_nch].name, ch, 63); g_ch[g_nch].name[63] = 0; g_ch[g_nch].cnt = 1; g_nch++; } }

static GMainLoop *g_loop; static int g_quit_on_done;
static void on_data(const lcm_recv_buf_t *rbuf, const char *ch, void *user)
{ (void) ch; (void) user;
  if (rbuf->data_size < (int) sizeof(hdr_t)) return;
  hdr_t h; memcpy(&h, rbuf->data, sizeof(h));
  push_lat(now_ns() - h.ts_ns);
  __atomic_add_fetch(&g_recv, 1, __ATOMIC_RELAXED); }
static void on_done(const lcm_recv_buf_t *rbuf, const char *ch, void *user)
{ (void) rbuf; (void) ch; (void) user;
  print_lat("lat");
  if (g_quit_on_done && g_loop) g_main_loop_quit(g_loop); }

/* 与 lcm_tunnel/src/lcm_util.c 一致的挂载逻辑 */
static int on_lcm_ready(GIOChannel *src, GIOCondition cond, void *ud)
{ (void) src; (void) cond; lcm_handle((lcm_t *) ud); return TRUE; }
static gboolean on_lcm_timer(void *ud)
{ lcm_handle_timeout((lcm_t *) ud, 10); return TRUE; }
static void attach_lcm(lcm_t *lcm)
{ int fd = lcm_get_fileno(lcm);
  printf("attach: lcm_get_fileno()=%d -> %s\n", fd,
         fd >= 0 ? "G_IO_IN (event driven)" : "g_timeout_add(10ms) fallback");
  if (fd >= 0) { GIOChannel *ioc = g_io_channel_unix_new(fd);
      g_io_add_watch(ioc, G_IO_IN, (GIOFunc) on_lcm_ready, lcm); }
  else g_timeout_add(10, on_lcm_timer, lcm);
  fflush(stdout); }

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc < 3) { fprintf(stderr, "usage: %s {sub|pub|cpu|poll} <url> ...\n", argv[0]); return 1; }
    const char *mode = argv[1], *url = argv[2];
    lcm_t *lcm = lcm_create(url);
    if (!lcm) { fprintf(stderr, "lcm_create(%s) failed\n", url); return 1; }

    if (!strcmp(mode, "pub")) {
        int count = argc > 3 ? atoi(argv[3]) : 300;
        double rate = argc > 4 ? atof(argv[4]) : 100.0;
        uint64_t period = (uint64_t) (1e9 / rate);
        double t0 = now_ns(), c0 = cpu_s(), next = now_ns();
        uint8_t buf[64]; memset(buf, 0, sizeof buf);
        for (int i = 0; i < count; i++) {
            hdr_t h; h.seq = i; h.ts_ns = now_ns();
            memcpy(buf, &h, sizeof h);
            lcm_publish(lcm, CH_DATA, buf, sizeof buf);
            next += period;
            int64_t sl = (int64_t) next - (int64_t) now_ns();
            if (sl > 0) usleep(sl / 1000);
        }
        usleep(200000);
        uint8_t x = 0; lcm_publish(lcm, CH_DONE, &x, 1);
        double wall = (now_ns() - t0) * 1e-9, cpu = cpu_s() - c0;
        printf("pub: %d msgs @ %.0f Hz wall=%.2fs cpu=%.3fs => %.1f%%\n",
               count, rate, wall, cpu, cpu / wall * 100);
        return 0;
    }
    if (!strcmp(mode, "sub")) {
        int count = argc > 3 ? atoi(argv[3]) : 300;
        const char *how = argc > 4 ? argv[4] : "glib";
        double c0 = cpu_s(), t0 = now_ns();
        lcm_subscribe(lcm, CH_DATA, on_data, NULL);
        lcm_subscribe(lcm, CH_DONE, on_done, NULL);
        if (!strcmp(how, "loop")) {
            uint64_t deadline = now_ns() + 20ULL * 1000000000ULL;
            printf("sub(loop): waiting %d msgs\n", count);
            while (g_recv < (uint64_t) count && now_ns() < deadline) lcm_handle(lcm);
        } else {
            g_loop = g_main_loop_new(NULL, FALSE); g_quit_on_done = 1;
            attach_lcm(lcm);
            printf("sub(glib): waiting %d msgs\n", count);
            g_main_loop_run(g_loop);
        }
        if (!g_printed) print_lat("lat");
        double wall = (now_ns() - t0) * 1e-9, cpu = cpu_s() - c0;
        printf("sub: wall=%.2fs cpu=%.3fs => %.2f%%\n", wall, cpu, cpu / wall * 100);
        return 0;
    }
    if (!strcmp(mode, "monitor")) {
        double secs = argc > 3 ? atof(argv[3]) : 3.0;
        const char *re = argc > 4 ? argv[4] : ".*";
        lcm_subscribe(lcm, re, on_mon, NULL);
        double t0 = now_ns(), tend = t0 + secs * 1e9;
        while (now_ns() < tend) lcm_handle_timeout(lcm, 100);
        printf("monitor %s: %.1fs total=%llu (%.0f msg/s), %d channels\n", url, secs,
               (unsigned long long) g_recv, g_recv / secs, g_nch);
        for (int i = 0; i < g_nch; i++)
            printf("   %-28s %8llu  (%.1f/s)\n", g_ch[i].name,
                   (unsigned long long) g_ch[i].cnt, g_ch[i].cnt / secs);
        fflush(stdout);
        return 0;
    }
    if (!strcmp(mode, "cpu") || !strcmp(mode, "poll")) {
        double secs = argc > 3 ? atof(argv[3]) : 3.0;
        int period = argc > 4 ? atoi(argv[4]) : 10;
        double c0 = cpu_s(), t0 = now_ns(), tend = t0 + secs * 1e9;
        uint8_t buf[64]; memset(buf, 0, sizeof buf);
        if (!strcmp(mode, "poll")) lcm_subscribe(lcm, CH_DATA, on_data, NULL);
        while (now_ns() < tend) {
            if (!strcmp(mode, "cpu")) {
                hdr_t h; h.seq = 0; h.ts_ns = now_ns();
                memcpy(buf, &h, sizeof h);
                lcm_publish(lcm, CH_DATA, buf, sizeof buf);
                usleep(10000);
            } else lcm_handle_timeout(lcm, period);
        }
        if (!g_printed) print_lat("lat");
        double wall = (now_ns() - t0) * 1e-9, cpu = cpu_s() - c0;
        printf("%s: wall=%.2fs cpu=%.3fs => %.2f%%  recv=%llu\n", mode, wall, cpu,
               cpu / wall * 100, (unsigned long long) g_recv);
        return 0;
    }
    fprintf(stderr, "unknown mode %s\n", mode);
    return 1;
}
