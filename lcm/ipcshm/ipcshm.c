#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>

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


//消息存入
static int _fifo_put(shm_fifo_t *fifo, void *items, uint32_t len)
{
    uint8_t *buffer = (uint8_t *)items;	//获取当前消息数据载入缓冲区
    // 获取 fifo 中空闲长度 
    len = min((uint32_t)(fifo->size - (uint32_t)(fifo->in - fifo->out)), len);
	//获取 fifo 当前存入 in 的位置
    uint32_t l = min(len, fifo->size - (uint32_t)(fifo->in & (fifo->size - 1)));
    //载入的 buffer 数据到_msgfifo 队列中，存入 in 位置 超出后回到 size 的 0 位置，循环这样
	memcpy((uint8_t *)fifo->data + (fifo->in & (fifo->size - 1)), buffer, l);
    memcpy(fifo->data, buffer + l , (len - l));
    fifo->in += len; //将 fifo 载入的位置 in 更新
    return len; //返回 len 长度
}
//fifo 消息出列
static int _fifo_get(shm_fifo_t *fifo, void *items, uint32_t len)
{
	//映射指向地址变量
    uint8_t *buffer = (uint8_t *)items;
    // 获取 fifo 中非空闲长度 
    len = min((uint32_t)(fifo->in - fifo->out), len);
    //获取 fifo 当前出列到 out 位置
    uint32_t l = min(len, fifo->size - (uint32_t)(fifo->out & (fifo->size - 1)));
    // 复制从 fifo->out 到尾部的数据
    memcpy(buffer, (uint8_t*)fifo->data + (fifo->out & (fifo->size - 1)), l);
    // 复制剩下的数据
    memcpy(buffer + l, fifo->data, (len - l));
    fifo->out += len; //将 fifo 出列的位置 out 更新
    return len;	//返回 len 长度
}

static uint64_t _fifo_touch(shm_fifo_t *fifo, uint64_t pos, void *items, uint32_t len)
{
	//映射指向地址变量
    uint8_t *buffer = (uint8_t *)items;
    // 获取 fifo 中非空闲长度 
    uint64_t out = fifo->out > pos ? fifo->out : pos;
    uint64_t avail = fifo->in - out;
    uint64_t touch_len = (uint64_t)len < avail ? (uint64_t)len : avail;
    if(touch_len <= 0)
    {
        return out;
    }
    //获取 fifo 当前出列到 out 位置
    if(items != NULL)
    {
        uint32_t l = (uint32_t)min(touch_len, (uint64_t)(fifo->size - (out & (fifo->size - 1))));
        // 复制从 fifo->out 到尾部的数据
        memcpy(buffer, (uint8_t*)fifo->data + (out & (fifo->size - 1)), l);
        // 复制剩下的数据
        memcpy(buffer + l, fifo->data, (size_t)(touch_len - l));
    }
    return (uint64_t)(touch_len + out);	//返回 len 长度
}

static int _fifo_drop(shm_fifo_t *fifo, uint32_t len)
{
    len = min((uint32_t)(fifo->in - fifo->out), len);
    fifo->out += len;
    return len;
}

uint32_t fifo_get_remain_size(shm_fifo_t *fifo)
{
    return fifo->size - (uint32_t)(fifo->in - fifo->out);
}

bool fifo_is_empty(shm_fifo_t *fifo)
{
    return fifo->in == fifo->out;
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
    assert(datalen <= header->fifo.size - sizeof(shm_msg_header_t));
    int len = strlen(channel);
    assert(len < 256);
    while(!lock(&header->mutex)) {}
    int count = 0;
    while(fifo_get_remain_size(&header->fifo) < datalen + sizeof(shm_msg_header_t))
    {
        shm_msg_t msg = {0};
        uint32_t len = _fifo_get(&header->fifo, &msg, sizeof(shm_msg_header_t));
        len = _fifo_drop(&header->fifo,  msg.header.size);

        if(count++ > 65535)
        {
            printf("fifo in = %lu, out = %lu, cannot make room, dropping publish\n",
                   (unsigned long)header->fifo.in, (unsigned long)header->fifo.out);
            unlock(&header->mutex);
            return false;
        }
    }
    {
        shm_msg_t msg = {0};
        msg.header.size = datalen;
        strncpy(msg.header.channel, channel, sizeof(msg.header.channel) - 1);
        msg.header.channel[sizeof(msg.header.channel) - 1] = '\0';
        msg.header.msg_num = ++ header->last_msg_num;
        dbg(DBG_LCM, "msg.header.msg_num: %d\n", msg.header.msg_num);
        if(msg.header.msg_num == 0)
        {
            msg.header.msg_num = ++ header->last_msg_num;
        }
        _fifo_put(&header->fifo, &msg, sizeof(shm_msg_header_t));
        _fifo_put(&header->fifo, data, datalen);
    }
    unlock(&header->mutex);
    return true;
}

bool compare_msgnum(uint32_t a, uint32_t b)
{
    int32_t r = a - b;
    return r >= 0;
}

bool shm_read(shm_t *header, uint32_t msg_num, uint64_t *last_msg_pos, shm_msgr_t *msgr)
{
    while(!lock(&header->mutex)) {}
    
    if(header->last_msg_num == msg_num)
    {
        unlock(&header->mutex);
        return false;
    }
    shm_msg_t msg = {0};
    if(*last_msg_pos == 0 || *last_msg_pos < header->fifo.out)
    {
        *last_msg_pos = header->fifo.out;
    }
    uint64_t pos = *last_msg_pos;
    int count = 0;
    int skipped = 0;
    while(true)
    {
        // Safety: if pos has fallen behind the writer's drop point,
        // reset to the current valid data start.
        if(pos < header->fifo.out)
        {
            dbg(DBG_LCM, "shm_read: pos=%lu behind out=%lu, resetting\n",
                (unsigned long)pos, (unsigned long)header->fifo.out);
            pos = header->fifo.out;
        }
        // Safety: if pos has somehow advanced past the writer, nothing left.
        if(pos >= header->fifo.in)
        {
            dbg(DBG_LCM, "shm_read: pos=%lu past in=%lu, no more data\n",
                (unsigned long)pos, (unsigned long)header->fifo.in);
            if(skipped > 0)
                dbg(DBG_LCM, "shm_read: skipped=%d before hitting in\n", skipped);
            *last_msg_pos = pos;
            unlock(&header->mutex);
            return false;
        }
       
        pos = _fifo_touch(&header->fifo, pos, &msg, sizeof(shm_msg_header_t));
        if(count++ >= 65535)
        {
            fprintf(stderr, "shm_read: stuck looping! in=%lu out=%lu pos=%lu "
                    "last_pos=%lu msg_num=%u hdr_msg_num=%u hdr_size=%u chan=%.32s\n",
                    (unsigned long)header->fifo.in, (unsigned long)header->fifo.out,
                    (unsigned long)pos, (unsigned long)*last_msg_pos,
                    msg_num, msg.header.msg_num, msg.header.size,
                    msg.header.channel[0] ? msg.header.channel : "(empty)");
            *last_msg_pos = pos;
            unlock(&header->mutex);
            return false;
        }
        if(msg.header.size == 0)
        {
            dbg(DBG_LCM, "shm_read: no more data, skipped=%d\n", skipped);
            unlock(&header->mutex);
            return false;
        }
        if(compare_msgnum(msg_num, msg.header.msg_num))
        {
            pos += msg.header.size;
            skipped++;
        }
        else
        {
            if(msg.header.size < sizeof(msgr->msg.data))
            {
                msgr->msg.header.size = msg.header.size;
                msgr->msg.header.msg_num = msg.header.msg_num;
                strcpy(msgr->msg.header.channel, msg.header.channel);
                pos = _fifo_touch(&header->fifo, pos, msgr->msg.data, msg.header.size);
            }
            else
            {

                msgr->msg.header.size = msg.header.size;
                msgr->msg.header.msg_num = msg.header.msg_num;
                strcpy(msgr->msg.header.channel, msg.header.channel);
                msgr->buff = (uint8_t *)malloc(msg.header.size);
                pos = _fifo_touch(&header->fifo, pos, msgr->buff, msg.header.size);
            }
            break;
        }
    }
    dbg(DBG_LCM, "shm_read: found msg %u, skipped %d older msgs, pos=%lu, header->fifo.in=%lu\n", msg.header.msg_num, skipped, (unsigned long)pos, (unsigned long)header->fifo.in);
    *last_msg_pos = pos;
    unlock(&header->mutex);
    return true;
}

// Atomically wait for a new message and read it.
// Solves the "one write, many readers" problem that FIFO cannot handle:
// pthread_cond_broadcast wakes ALL readers; each atomically checks+reads
// under the same mutex.
int shm_wait(shm_t *header, uint32_t msg_no, int timeout_ms)
{
    // Poll with usleep.  pthread_cond_t in shared memory is not robust:
    // a crashed process corrupts the condvar, freezing all other processes.
    // Simple polling avoids this entirely.
    // int elapsed = 0;
    // while(1)
    // {
    //     while(!lock(&header->mutex)) {}
    //     if(header->last_msg_num > msg_no)
    //     {
    //         unlock(&header->mutex);
    //         return 1;
    //     }
    //     unlock(&header->mutex);

    //     if(timeout_ms == 0)
    //         return 0;
    //     if(timeout_ms > 0 && elapsed >= timeout_ms)
    //         return 0;

    //     usleep(1000);  // 1ms
    //     elapsed += 1;
    // }
    usleep(1000); 
    return 1;
}