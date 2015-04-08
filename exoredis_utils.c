#include "exoredis.h"
#include "exoredis_server.h"
#include "exoredis_hash.h"
#include "exordb.h"

/* Define prefix strings for the data types */
char prefix_string[MAX_DATA_TYPE] = {'+','-',':','$','*'};

#define TRAIL_STRING_LEN 2
char trail_string[TRAIL_STRING_LEN] = "\r\n";

#define SIMPLE_STRING_MAX_LEN 4
char simple_string[SIMPLE_STRING_MAX][SIMPLE_STRING_MAX_LEN] = {"OK "};

#define ERROR_STRING_MAX_LEN 5
char error_string[ERROR_STRING_MAX][ERROR_STRING_MAX_LEN] = {"ERR "};

#if 0
/*
 * exoredis_handle_get():
 * This function handles the REDIS protocol GET request
 * It looks up the DB file passed in as argument, and:
 * 1. If key exists, returns the value if the value is string. 
 * 2. If key exists, error is returned if value is not string
 * 3. If key does not exist, returns a bulk string nil.
 */
#endif

unsigned char *
exoredis_parse_args (unsigned char *key,
                     int args_len,
                     int *key_len,
                     int *value_len)
{
    unsigned char *ptr = key;
    *key_len = 0;
    *value_len = 0;

    /* Check for key value format */
    while ((*ptr != ' ') && (*key_len < args_len)) {
        ptr++; (*key_len)++;
    }

    *value_len = args_len - *key_len;
    while ((*ptr == ' ') && (*value_len > 0)) {
        ptr++; (*value_len)--;
    }

    return ptr;
}

void exoredis_resp_error(char *msg)
{
    char buf[MAX_REQ_RESP_MSGLEN];
    char *ptr = buf;
    int buf_len = 0;

    memset(ptr, 0, MAX_REQ_RESP_MSGLEN);
    /* OK to use string operations since these are always simple strings */
    *ptr++ = prefix_string[ERROR_DATA_TYPE];
    buf_len++;
    memcpy(ptr, error_string[ERROR_STRING_ERR], 
           strlen(error_string[ERROR_STRING_ERR]));
    ptr += strlen(error_string[ERROR_STRING_ERR]);
    buf_len += strlen(error_string[ERROR_STRING_ERR]);
    memcpy(ptr, msg, strlen(msg));
    ptr += strlen(msg);
    buf_len += strlen(msg);
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_resp_ok(void)
{
    char buf[MAX_REQ_RESP_MSGLEN];
    char *ptr = buf;
    int buf_len = 0;

    memset(ptr, 0, MAX_REQ_RESP_MSGLEN);
    /* OK  to use string operations since these are always simple strings */
    *ptr++ = prefix_string[SIMPLE_STRING_DATA_TYPE];
    buf_len++;
    memcpy(ptr, simple_string[SIMPLE_STRING_OK], 
            strlen(simple_string[SIMPLE_STRING_OK]));

    ptr += strlen(simple_string[SIMPLE_STRING_OK]);
    buf_len += strlen(simple_string[SIMPLE_STRING_OK]);

    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_resp_bulk_string (unsigned char *msg,
                                int len)
{
    unsigned char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    unsigned char *ptr = buf;

    int buf_len = 0;

    /* OK to use string operations since these are always simple strings */
    *ptr++ = prefix_string[BULK_STRING_DATA_TYPE];
    buf_len++;
    sprintf((char *)ptr, "%d", len);
    ptr += sizeof(len);
    buf_len += sizeof(len);
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    memcpy(ptr, msg, len);
    ptr += len;
    buf_len += len;
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_handle_set (unsigned char *key,
                          int args_len)
{
    unsigned char *ptr = NULL;
    int key_len = 0;
    int value_len = 0;

    /* Format : SET key string */
    ptr = exoredis_parse_args(key, args_len, &key_len, &value_len);
    if (!(key_len) || !(value_len)) {
        exoredis_resp_error("Incorrect arguments to command\n");
        return;
    }

    printf("SET Command: SET %s %d %s %d \n", key, key_len, ptr, value_len);
    /* Write the value into hash */
    exoredis_create_update_he(key, key_len, ptr, value_len);
    exoredis_resp_ok();
    return;
}


void exoredis_handle_get (unsigned char *key,
                          int args_len)
{
    int key_len = 0;
    int value_len = 0;
    unsigned char *value = NULL;

    /* Format : SET key string */
    exoredis_parse_args(key, args_len, &key_len, &value_len);
    if (!(key_len) || value_len) {
        exoredis_resp_error("Incorrect arguments to command\n");
        return;
    }

    printf("GET Command: GET %s %d\n", key, key_len);
    
    /* Write the value into hash */
    exoredis_read_he(key, key_len, &value, &value_len);
    
    /* Send Bulk String */
    exoredis_resp_bulk_string(value, value_len);

    /* Deallocate memory */
    free(value);

    return;
}

void exoredis_handle_save (void)
{
    printf("SAVE Command: %s\n", "+OK");
    
    /* Dump the contents of the hash table into the DB File */
    exoredis_feed_ht_to_io();

    /* For all other entries that exist, look for duplicates and delete */
    
    return;
}
