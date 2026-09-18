#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/eventfd.h>
#define LCM_SHM_NOTIFY_FD 1
#endif

#include <glib.h>

#include "dbg.h"
#include "lcm.h"
#include "lcm_internal.h"
#include "udpm_util.h"

#include "ipcshm/ipcshm.h"

#define DEFAULT_SHM_SIZE    (4*1024*1024)

// Upper bound on the messages read by a single lcm_handle() call, so a chatty
// channel cannot starve the rest of the application's event loop.  Leftover
// messages simply re-arm the notify fd.
#define MAX_MSGS_PER_HANDLE 256

// How long the notify thread sleeps in the futex wait before re-checking the
// shutdown flag.  Only a safety net: shm_notify_waiters() interrupts the sleep
// right away, this just bounds the worst case (the wake racing with the thread
// entering the futex).
#define NOTIFY_WAIT_MS      50


typedef struct _lcm_provider_t lcm_shm_t;
struct _lcm_provider_t {
    lcm_t *lcm;
    shm_t *shm;
    uint32_t msg_no;
    uint64_t last_msg_pos;

    // --- local (per process) wakeup machinery -------------------------------
    // The cross-process signalling primitive is the futex on
    // shm_header_t::last_msg_num.  A futex has no file descriptor, so a thread
    // bridges it to an eventfd that an event loop (glib, poll, epoll, ...) can
    // wait on: it sleeps in shm_wait() and kicks the eventfd whenever a message
    // newer than the last consumed one shows up.
    int notify_fd;              // -1 when unavailable
    pthread_t notify_thread;
    volatile int notify_stop;
    pthread_mutex_t notify_lock;  // guards the lazy start below
};

// wrap-around-aware "newer than" test (valid for differences < 2^31)
static inline int msg_num_newer(uint32_t a, uint32_t b)
{
    return (int32_t) (a - b) > 0;
}

// msg_no is written by the thread calling lcm_handle() and read by the notify
// thread, so all accesses go through the atomics.
static inline uint32_t reader_msg_no(lcm_shm_t *self)
{
    return __atomic_load_n(&self->msg_no, __ATOMIC_RELAXED);
}

static inline void set_reader_msg_no(lcm_shm_t *self, uint32_t msg_no)
{
    __atomic_store_n(&self->msg_no, msg_no, __ATOMIC_RELAXED);
}

static int shm_has_pending(lcm_shm_t *self)
{
    return msg_num_newer(shm_get_msg_num(self->shm), reader_msg_no(self));
}

// eventfd is a counter: a single write() makes it readable, a read() returns
// the accumulated count and clears it.  Over-signalling is harmless, a missed
// signal is not - so the thread below posts once per observed new message and
// handle() re-arms while messages are still pending.
static void notify_post(lcm_shm_t *self)
{
    if (self->notify_fd < 0)
        return;
    uint64_t one = 1;
    if (write(self->notify_fd, &one, sizeof(one)) < 0 && errno != EAGAIN)
        perror("lcm_shm: write(notify_fd)");
}

static void notify_ack(lcm_shm_t *self)
{
    if (self->notify_fd < 0)
        return;
    uint64_t v;
    while (read(self->notify_fd, &v, sizeof(v)) > 0) {
        // counter cleared; anything posted from now on makes us readable again
    }
}

static void *notify_thread_func(void *arg)
{
    lcm_shm_t *self = (lcm_shm_t *) arg;

    // Our own cursor, advanced on every wakeup.  It must NOT stay pinned to the
    // application's cursor: waiting for "head newer than the app cursor" would
    // return immediately for as long as the application is behind - and a
    // process that publishes without reading (or a slow reader) is behind
    // essentially all the time - which turns this thread into a busy loop
    // burning a whole core.
    //
    // It does start at the application's cursor, so messages that were already
    // published (and not yet read) when we started still produce a wakeup.
    uint32_t seen = reader_msg_no(self);

    while (!self->notify_stop) {
        // Sleep until the ring head moves past what we have already signalled.
        if (shm_wait_stop(self->shm, seen, NOTIFY_WAIT_MS, &self->notify_stop) != 1)
            continue;  // timeout, or asked to stop
        seen = shm_get_msg_num(self->shm);
        if (shm_has_pending(self))  // the application still has reading to do
            notify_post(self);
    }
    return NULL;
}

// Start the eventfd + notify thread on demand.  Processes that never wait on
// the fd (they publish, or call lcm_handle()) do not need either, and skipping
// them keeps their CPU cost at zero.  Idempotent.
static void notify_start(lcm_shm_t *self)
{
#ifdef LCM_SHM_NOTIFY_FD
    pthread_mutex_lock(&self->notify_lock);
    if (self->notify_fd >= 0)
        goto done;

    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) {
        perror("eventfd (lcm_shm)");
        goto done;
    }
    self->notify_fd = fd;
    if (pthread_create(&self->notify_thread, NULL, notify_thread_func, self) != 0) {
        perror("pthread_create (lcm_shm notify)");
        close(fd);
        self->notify_fd = -1;
    } else {
        dbg(DBG_LCM, "lcm_shm: notify_fd=%d\n", self->notify_fd);
    }
done:
    pthread_mutex_unlock(&self->notify_lock);
#else
    (void) self;
#endif
}


void lcm_shm_destroy(lcm_shm_t *lcm)
{
    dbg(DBG_LCM, "lcm_shm_destroy: closing lcm context\n");

    pthread_mutex_lock(&lcm->notify_lock);
    int started = (lcm->notify_fd >= 0);
    if (started) {
        lcm->notify_stop = 1;
        shm_notify_waiters(lcm->shm);  // interrupt the futex wait
    }
    pthread_mutex_unlock(&lcm->notify_lock);

    if (started) {
        pthread_join(lcm->notify_thread, NULL);
        close(lcm->notify_fd);
        lcm->notify_fd = -1;
    }
    pthread_mutex_destroy(&lcm->notify_lock);

    shm_deinit(lcm->shm);
    free(lcm);
}

static int lcm_shm_get_fileno(lcm_shm_t *lcm)
{
    notify_start(lcm);   // for event loops (glib / poll / epoll)
    return lcm->notify_fd;
}
static int lcm_shm_subscribe(lcm_shm_t *lcm, const char *c) { (void)lcm;(void)c; return 0; }
static int lcm_shm_unsubscribe(lcm_shm_t *lcm, const char *c) { (void)lcm;(void)c; return 0; }

static int lcm_shm_publish(lcm_shm_t *lcm, const char *channel, const void *data, unsigned int datalen)
{
    dbg(DBG_LCM, "lcm_shm_publish: %s, %d\n", channel, datalen);
    return shm_publish(lcm->shm, channel, data, datalen) ? 0 : -1;
}

// Consume everything that is pending.  Messages on channels with no local
// subscriber are taken off the ring without being dispatched, but they still
// count as progress: the caller asked us to read the ring, and such messages
// may be the only thing that ever arrives (e.g. a process that publishes on a
// channel it does not subscribe to).  Blocking again in that case would
// deadlock.
//
// Returns the number of messages taken off the ring.
static int lcm_shm_read_available(lcm_shm_t *self)
{
    int nread = 0;
    int dispatched = 0;

    while (nread < MAX_MSGS_PER_HANDLE) {
        shm_msgr_t msgr = {0};
        if (!shm_read(self->shm, reader_msg_no(self), &self->last_msg_pos, &msgr))
            break;  // reached the latest message

        set_reader_msg_no(self, msgr.msg.header.msg_num);
        nread++;

        if (lcm_try_enqueue_message(self->lcm, msgr.msg.header.channel)) {
            lcm_recv_buf_t rbuf;
            rbuf.data       = msgr.buff ? (void *) msgr.buff : (void *) msgr.msg.data;
            rbuf.data_size  = msgr.msg.header.size;
            rbuf.recv_utime = lcm_timestamp_now();
            rbuf.lcm        = self->lcm;
            lcm_dispatch_handlers(self->lcm, &rbuf, msgr.msg.header.channel);
            dispatched++;
        }
        if (msgr.buff)
            free(msgr.buff);
    }

    dbg(DBG_LCM, "lcm_shm_read_available: read %d, dispatched %d\n", nread, dispatched);
    return nread;
}

static int lcm_shm_handle(lcm_shm_t *self)
{
    int timeout_ms = lcm_get_handle_timeout(self->lcm);
    int64_t deadline = (timeout_ms > 0) ? lcm_timestamp_now() / 1000 + timeout_ms : -1;

    for (;;) {
        if (lcm_shm_read_available(self) > 0) {
            // Drop the wakeup that got us here, then re-arm if more is waiting:
            // a message published between the read loop and the ack must not
            // lose its wakeup.
            notify_ack(self);
            if (shm_has_pending(self))
                notify_post(self);
            return 0;
        }
        notify_ack(self);

        if (timeout_ms == 0)
            return 1;  // non-blocking call, nothing pending

        int left = -1;
        if (deadline >= 0) {
            left = (int) (deadline - lcm_timestamp_now() / 1000);
            if (left <= 0)
                return 1;
        }

        // Precise wait: futexes cannot lose a wakeup, the reader re-checks
        // last_msg_num after every wakeup.
        if (!shm_wait(self->shm, reader_msg_no(self), left)) {
            dbg(DBG_LCM, "lcm_shm_handle: timeout=%d, no msg\n", timeout_ms);
            return 1;  // timeout
        }
    }
}

static void shm_arg_parse(gpointer key, gpointer value, gpointer user)
{
    uint32_t *size = (uint32_t *)user;
    if (!strcmp((char *)key, "size")) {
        char *endptr = NULL;
        unsigned long val = strtoul((char *)value, &endptr, 0);
        if (endptr != value && val > 0 && val <= 256 * 1024 * 1024) *size = (uint32_t)val;
        else fprintf(stderr, "Warning: Invalid shm size \"%s\"\n", (char *)value);
    }
}

lcm_provider_t *lcm_shm_create(lcm_t *parent, const char *network, const GHashTable *args)
{
    uint32_t shm_size = DEFAULT_SHM_SIZE;
    if(args) g_hash_table_foreach((GHashTable *)args, shm_arg_parse, &shm_size);

    lcm_shm_t *self = (lcm_shm_t *)calloc(1, sizeof(lcm_shm_t));
    self->lcm = parent;
    self->last_msg_pos = 0;
    self->notify_fd = -1;
    self->shm = shm_create(shm_size, true);
    if(!self->shm) { perror("shm_create"); free(self); return NULL; }
    self->msg_no = shm_get_lastest_msg_num(self->shm);

    pthread_mutex_init(&self->notify_lock, NULL);
    // The eventfd + notify thread are started lazily by lcm_shm_get_fileno(),
    // i.e. only for applications that actually wait on a file descriptor.

    return self;
}

#ifdef WIN32
static lcm_provider_vtable_t shm_vtable;
#else
static lcm_provider_vtable_t shm_vtable = {
    .create = lcm_shm_create, .destroy = lcm_shm_destroy,
    .subscribe = lcm_shm_subscribe, .unsubscribe = lcm_shm_unsubscribe,
    .publish = lcm_shm_publish, .handle = lcm_shm_handle,
    .get_fileno = lcm_shm_get_fileno,
};
#endif

static lcm_provider_info_t shm_info;

void lcm_shm_provider_init(GPtrArray *providers)
{
    shm_info.name = "ipcshm";
    shm_info.vtable = &shm_vtable;
    g_ptr_array_add(providers, &shm_info);
}
