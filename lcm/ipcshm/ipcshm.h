#pragma once

#include <stdint.h>
#include <stdbool.h>

typedef struct shm_msg_header_t
{
    uint32_t size;
    uint32_t msg_num;
    char channel[256];
}shm_msg_header_t;

typedef struct shm_msg_t
{
    shm_msg_header_t header;
    char data[1024];
}shm_msg_t;
typedef struct shm_msgr_t
{
    shm_msg_t msg;
    uint8_t *buff;
}shm_msgr_t;

typedef struct shm_header_t shm_t;

shm_t * shm_create(uint32_t size, bool sharem);
void shm_deinit(shm_t *header);
bool shm_publish(shm_t *header, const char *channel, const void *data, uint32_t datalen);
bool shm_read(shm_t *header, uint32_t msg_num, uint64_t *last_msg_pos, shm_msgr_t *msgr);
int shm_get_lastest_msg_num(shm_t * header);

// Block until a message newer than msg_no arrives.
// timeout_ms: <0 = forever, 0 = non-blocking, >0 = N ms.
// Returns 1 when new data available, 0 on timeout.
int  shm_wait(shm_t *header, uint32_t msg_no, int timeout_ms);