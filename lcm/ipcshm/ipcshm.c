#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <linux/futex.h>
#include <sys/syscall.h>

#include "dbg.h"
#include "ipcshm.h"

#define max(a,b)    (((a) > (b)) ? (a) : (b)) 
#define min(a,b)    (((a) < (b)) ? (a) : (b)) 

typedef struct shm_fifo_t
{
    uint32_t size;
    uint64_t in;
    uint64_t out;
    uint8_t  data[0];
}shm_fifo_t;

#define SHM_MAGIC   0x12345678
#define MEM_MAGIC   0x87654321

typedef struct shm_header_t
{
    uint32_t magic;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    uint32_t last_msg_num;
    shm_fifo_t fifo;
}shm_header_t;


// ---- lock-free fifo access ------------------------------------------------
// in/out are 64-bit cumulative counters shared across processes and read
// lock-free by subscribers.  All accesses must therefore go through atomic
// 64-bit loads/stores (armv7-a provides ldrexd/strexd for these).  Writers
// are serialized by the producer mutex; a writer commits `in` with a release
// store only after a whole message has been written, so a reader that loads
// `in` with acquire semantics never observes a partially written message.

static inline uint64_t fifo_in(shm_fifo_t *fifo)
{
    return __atomic_load_n(&fifo->in, __ATOMIC_ACQUIRE);
}

static inline uint64_t fifo_out(shm_fifo_t *fifo)
{
    return __atomic_load_n(&fifo->out, __ATOMIC_ACQUIRE);
}

static inline void fifo_in_store(shm_fifo_t *fifo, uint64_t v)
{
    __atomic_store_n(&fifo->in, v, __ATOMIC_RELEASE);
}

static inline void fifo_out_store(shm_fifo_t *fifo, uint64_t v)
{
    __atomic_store_n(&fifo->out, v, __ATOMIC_RELEASE);
}

// Wrap-aware copy: read `len` bytes starting at cumulative position `pos`.
// Caller must guarantee pos+len <= committed `in`.
static void fifo_read_at(shm_fifo_t *fifo, uint64_t pos, void *buf, uint32_t len)
{
    uint8_t *b = (uint8_t *)buf;
    uint32_t off = (uint32_t)(pos & (fifo->size - 1));
    uint32_t l = min(len, fifo->size - off);
    memcpy(b, (uint8_t *)fifo->data + off, l);
    memcpy(b + l, fifo->data, len - l);
}

// Wrap-aware write: write `len` bytes starting at cumulative position `pos`.
// Does NOT advance `in`; the writer commits `in` afterwards.
static void fifo_write_at(shm_fifo_t *fifo, uint64_t pos, const void *buf, uint32_t len)
{
    const uint8_t *b = (const uint8_t *)buf;
    uint32_t off = (uint32_t)(pos & (fifo->size - 1));
    uint32_t l = min(len, fifo->size - off);
    memcpy((uint8_t *)fifo->data + off, b, l);
    memcpy(fifo->data, b + l, len - l);
}

bool lock(pthread_mutex_t *mutex)
{
    int r = pthread_mutex_lock(mutex);
    if(r == EOWNERDEAD)
    {
        perror("lock dead\n");
        pthread_mutex_consistent(mutex);
        pthread_mutex_unlock(mutex);
        return false;
    }
    return true;
}

void unlock(pthread_mutex_t *mutex)
{
    pthread_mutex_unlock(mutex);
}

// ---- futex helpers ---------------------------------------------------------
// last_msg_num doubles as the futex word: the publisher increments it and
// wakes waiters, subscribers sleep on it.  Unlike a shared-memory
// pthread_cond_t, the kernel owns the wait queue, so a crashed process
// cannot corrupt it and freeze the other processes.

static int futex_wait(uint32_t *uaddr, uint32_t expected, const struct timespec *timeout)
{
    return syscall(SYS_futex, uaddr, FUTEX_WAIT, expected, timeout, NULL, 0);
}

static int futex_wake(uint32_t *uaddr, int n)
{
    return syscall(SYS_futex, uaddr, FUTEX_WAKE, n, NULL, NULL, 0);
}

// wrap-around-aware "newer than" test (valid for differences < 2^31)
static bool msg_num_newer(uint32_t a, uint32_t b)
{
    return (int32_t)(a - b) > 0;
}

int shm_get_lastest_msg_num(shm_t * header)
{
    int r = 0;
    while(!lock(&header->mutex)) {}
    r = header->last_msg_num;
    unlock(&header->mutex);
    return r;
}

shm_t * shm_create(uint32_t size, bool sharem)
{
    if(sharem)
    {
        int shm_fd = shm_open("/ipcshm000", O_CREAT | O_RDWR, 0666);
        if (shm_fd == -1) {
            perror("shm_open");
            return NULL;
        }

        if (ftruncate(shm_fd, sizeof(shm_header_t) + size) == -1) {
            perror("ftruncate");
            close(shm_fd);
            return NULL;
        }

        shm_header_t *header = mmap(NULL, sizeof(shm_header_t) + size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (header == MAP_FAILED) {
            perror("mmap");
            close(shm_fd);
            return NULL;
        }

        close(shm_fd);

        if(header->magic != SHM_MAGIC)
        {
            header->magic = SHM_MAGIC;
            header->last_msg_num = 0;
            header->fifo.size = size;
            header->fifo.in = 0;
            header->fifo.out = 0;
            pthread_mutexattr_t attr;
            pthread_mutexattr_init(&attr);
            pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
            pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
            pthread_mutex_init(&header->mutex, &attr);
            pthread_mutexattr_destroy(&attr);

            pthread_condattr_t cattr;
            pthread_condattr_init(&cattr);
            pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
            pthread_cond_init(&header->cond, &cattr);
            pthread_condattr_destroy(&cattr);
        }
        return header;
    }
    else
    {
        shm_header_t *header = (shm_header_t *)malloc(sizeof(shm_header_t) + size);
        if(header == NULL)
        {
            return NULL;
        }
        header->magic = MEM_MAGIC;
        header->last_msg_num = 0;
        header->fifo.size = size;
        header->fifo.in = 0;
        header->fifo.out = 0;
        pthread_mutex_init(&header->mutex, NULL);
        pthread_cond_init(&header->cond, NULL);
        return header;
    }
}

void shm_deinit(shm_t *header)
{
    if(header->magic == SHM_MAGIC)
    {
        munmap(header, sizeof(shm_header_t) + header->fifo.size);
        // Do NOT shm_unlink here — other processes may still be using
        // the shared memory.  The kernel cleans up when the last process
        // exits (munmap + all fds closed).
    }
    else if(header->magic == MEM_MAGIC)
    {
        free(header);
    }
    else
    {
        assert(0);
    }
}

bool shm_publish(shm_t *header, const char *channel, const void *data, uint32_t datalen)
{
    uint32_t total = sizeof(shm_msg_header_t) + datalen;
    assert(datalen <= header->fifo.size - sizeof(shm_msg_header_t));
    int len = strlen(channel);
    assert(len < 256);

    // Serialize writers only.  Readers are lock-free, so holding this mutex
    // during the payload memcpy does NOT block subscribers.
    while(!lock(&header->mutex)) {}

    // Reserve the next message number (writers are serialized by the mutex).
    uint32_t msg_num = header->last_msg_num + 1;
    if (msg_num == 0)
        msg_num = 1;

    uint64_t in  = fifo_in(&header->fifo);
    uint64_t out = fifo_out(&header->fifo);

    // Make room by dropping the oldest messages (only when full).
    int count = 0;
    while (header->fifo.size - (uint32_t)(in - out) < total)
    {
        shm_msg_t old = {0};
        fifo_read_at(&header->fifo, out, &old, sizeof(shm_msg_header_t));
        out += sizeof(shm_msg_header_t) + old.header.size;
        if (++count > 65535)
        {
            // printf("fifo in=%lu out=%lu, cannot make room, dropping publish\n",
            //        (unsigned long)in, (unsigned long)out);
            unlock(&header->mutex);
            return false;
        }
    }
    fifo_out_store(&header->fifo, out);

    // Build the message header.
    shm_msg_t msg = {0};
    msg.header.size = datalen;
    strncpy(msg.header.channel, channel, sizeof(msg.header.channel) - 1);
    msg.header.channel[sizeof(msg.header.channel) - 1] = '\0';
    msg.header.msg_num = msg_num;
    dbg(DBG_LCM, "msg.header.msg_num: %d\n", msg.header.msg_num);

    // Write header + payload, then commit `in` (release) so readers never see
    // a partially written message.
    fifo_write_at(&header->fifo, in, &msg, sizeof(shm_msg_header_t));
    fifo_write_at(&header->fifo, in + sizeof(shm_msg_header_t), data, datalen);
    fifo_in_store(&header->fifo, in + total);

    // Publish the message number only AFTER the ring data is committed.
    __atomic_store_n(&header->last_msg_num, msg_num, __ATOMIC_RELEASE);

    unlock(&header->mutex);
    futex_wake(&header->last_msg_num, INT_MAX);
    return true;
}

bool compare_msgnum(uint32_t a, uint32_t b)
{
    int32_t r = a - b;
    return r >= 0;
}

bool shm_read(shm_t *header, uint32_t msg_num, uint64_t *last_msg_pos, shm_msgr_t *msgr)
{
    uint32_t latest = __atomic_load_n(&header->last_msg_num, __ATOMIC_ACQUIRE);
    if (!msg_num_newer(latest, msg_num))
        return false;

    uint64_t pos = *last_msg_pos;
    uint64_t out = fifo_out(&header->fifo);
    if (pos == 0 || pos < out)
        pos = out;

    int count = 0;
    for (;;)
    {
        // Re-read the floor each iteration: the publisher may have dropped
        // older messages (advancing `out`) while we were scanning.
        out = fifo_out(&header->fifo);
        if (pos < out)
            pos = out;

        uint64_t in = fifo_in(&header->fifo);
        if (pos >= in)
        {
            *last_msg_pos = pos;
            return false;  // no more committed data
        }

        shm_msg_t msg = {0};
        fifo_read_at(&header->fifo, pos, &msg, sizeof(shm_msg_header_t));
        if (++count > 65535)
        {
            fprintf(stderr, "shm_read: stuck looping! in=%lu out=%lu pos=%lu msg_num=%u\n",
                    (unsigned long)in, (unsigned long)out, (unsigned long)pos, msg_num);
            *last_msg_pos = pos;
            return false;
        }
        if (msg.header.size == 0)
        {
            *last_msg_pos = pos;
            return false;
        }

        if (compare_msgnum(msg_num, msg.header.msg_num))
        {
            // Already-seen message: skip it.
            pos += sizeof(shm_msg_header_t) + msg.header.size;
            continue;
        }

        // Found the first message newer than msg_num.  Copy it out.
        uint64_t msg_pos = pos;
        msgr->msg.header = msg.header;
        if (msg.header.size < sizeof(msgr->msg.data))
            fifo_read_at(&header->fifo, pos + sizeof(shm_msg_header_t), msgr->msg.data, msg.header.size);
        else
        {
            msgr->buff = (uint8_t *)malloc(msg.header.size);
            fifo_read_at(&header->fifo, pos + sizeof(shm_msg_header_t), msgr->buff, msg.header.size);
        }

        // If the publisher dropped this message while we were copying it,
        // discard and restart from the new floor (our copy may be stale).
        if (msg_pos < fifo_out(&header->fifo))
        {
            if (msgr->buff) { free(msgr->buff); msgr->buff = NULL; }
            pos = fifo_out(&header->fifo);
            continue;
        }

        pos += sizeof(shm_msg_header_t) + msg.header.size;
        *last_msg_pos = pos;
        return true;
    }
}

// Wait until a message newer than msg_no is available.
// Sleeps on the last_msg_num futex word: the publisher increments it and
// calls futex_wake, waking us with no polling delay.  Crash-safe because
// the kernel owns the wait queue (a dead process cannot corrupt it).
int shm_wait(shm_t *header, uint32_t msg_no, int timeout_ms)
{
    uint32_t cur = __atomic_load_n(&header->last_msg_num, __ATOMIC_ACQUIRE);
    if (msg_num_newer(cur, msg_no))
        return 1;

    if (timeout_ms == 0)
        return 0;  // non-blocking

    struct timespec ts;
    struct timespec *pts = NULL;
    if (timeout_ms > 0) {
        ts.tv_sec  = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        pts = &ts;
    }

    for (;;) {
        // FUTEX_WAIT atomically re-checks *uaddr == cur before sleeping,
        // so a message arriving between the check above and this call is
        // not missed (it returns EAGAIN instead of sleeping).
        int ret = futex_wait(&header->last_msg_num, cur, pts);
        if (ret == 0 || (ret == -1 && errno == EAGAIN)) {
            cur = __atomic_load_n(&header->last_msg_num, __ATOMIC_ACQUIRE);
            if (msg_num_newer(cur, msg_no))
                return 1;
            continue;  // spurious wakeup — wait again
        }
        return 0;  // ETIMEDOUT / EINTR / error
    }
}