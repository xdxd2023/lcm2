#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <glib.h>

#include "dbg.h"
#include "lcm.h"
#include "lcm_internal.h"
#include "udpm_util.h"

#include "ipcshm/ipcshm.h"

#define DEFAULT_SHM_SIZE    (4*1024*1024)


typedef struct _lcm_provider_t lcm_shm_t;
struct _lcm_provider_t {
    lcm_t *lcm;
    shm_t *shm;
    uint32_t msg_no;
    uint64_t last_msg_pos;
};


void lcm_shm_destroy(lcm_shm_t *lcm)
{
    dbg(DBG_LCM, "lcm_shm_destroy: closing lcm context\n");
    shm_deinit(lcm->shm);
    free(lcm);
}

static int lcm_shm_get_fileno(lcm_shm_t *lcm) { (void)lcm; return -1; }
static int lcm_shm_subscribe(lcm_shm_t *lcm, const char *c) { (void)lcm;(void)c; return 0; }
static int lcm_shm_unsubscribe(lcm_shm_t *lcm, const char *c) { (void)lcm;(void)c; return 0; }

static int lcm_shm_publish(lcm_shm_t *lcm, const char *channel, const void *data, unsigned int datalen)
{
    dbg(DBG_LCM, "lcm_shm_publish: %s, %d\n", channel, datalen);
    return shm_publish(lcm->shm, channel, data, datalen) ? 0 : -1;
}

static int lcm_shm_handle(lcm_shm_t *lcm)
{
    int timeout_ms = lcm_get_handle_timeout(lcm->lcm);

    // Wait until new data is available.
    if(!shm_wait(lcm->shm, lcm->msg_no, timeout_ms))
    {
        dbg(DBG_LCM, "lcm_shm_handle: timeout=%d, no msg\n", timeout_ms);
        return 0;  // timeout
    }

    // Read messages, skipping channels with no subscribers.
    while (1)
    {
        shm_msgr_t msgr = {0};
        if(!shm_read(lcm->shm, lcm->msg_no, &lcm->last_msg_pos, &msgr))
            break;  // reached the latest message

        if (!lcm_try_enqueue_message(lcm->lcm, msgr.msg.header.channel))
        {
            lcm->msg_no = msgr.msg.header.msg_num;
            if(msgr.buff) free(msgr.buff);
            continue;  // skip, try next
        }

        lcm->msg_no = msgr.msg.header.msg_num;
        lcm_recv_buf_t rbuf;
        rbuf.data      = msgr.buff ? msgr.buff : msgr.msg.data;
        rbuf.data_size = msgr.msg.header.size;
        rbuf.recv_utime = lcm_timestamp_now();
        rbuf.lcm       = lcm->lcm;
        lcm_dispatch_handlers(lcm->lcm, &rbuf, msgr.msg.header.channel);
        if(msgr.buff) free(msgr.buff);
        break;
    }
    return 0;
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
    self->shm = shm_create(shm_size, true);
    if(!self->shm) { perror("shm_create"); free(self); return NULL; }
    self->msg_no = shm_get_lastest_msg_num(self->shm);
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
