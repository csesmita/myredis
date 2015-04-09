#include "exoredis.h"
#include "exoredis_server.h"
#include "exoredis_hash.h"
#include "exordb.h"
#include <ctype.h>

/* Define prefix strings for the data types */
char prefix_string[MAX_DATA_TYPE] = {'+','-',':','$','*'};

#define TRAIL_STRING_LEN 2
char trail_string[TRAIL_STRING_LEN] = "\r\n";

#define SIMPLE_STRING_MAX_LEN 4
char simple_string[SIMPLE_STRING_MAX][SIMPLE_STRING_MAX_LEN] = {"OK "};

#define ERROR_STRING_MAX_LEN 5
char error_string[ERROR_STRING_MAX][ERROR_STRING_MAX_LEN] = {"ERR "};


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

exoredis_return_codes
exoredis_parse_key_arg (unsigned char **key,
                        int *args_len,
                        int *key_len)
{
    *key_len = 0;
    unsigned char *ptr = *key;

    /* Check for key value format */
    while ((*ptr == ' ') && (*args_len > 0)) {
         ptr++; (*args_len)--;
    }

    while (*ptr != ' ' && (*args_len > 0)) {
        ptr++; (*args_len)--; (*key_len)++;
    }

    return (*key_len == 0)? EXOREDIS_ARGS_MISSING: EXOREDIS_OK;
}

exoredis_return_codes
exoredis_parse_value_arg (unsigned char **value,
                          int *args_len,
                          int *value_len)
{
    *value_len = *args_len;
    while ((**value == ' ') && (*value_len > 0)) {
        (*value)++; (*value_len)--; (*args_len)--;
    }

    return (*value_len == 0)? EXOREDIS_ARGS_MISSING: EXOREDIS_OK;
}

exoredis_return_codes
exoredis_parse_bitoffset_arg (unsigned char **bo,
                              int *args_len,
                              unsigned int *bo_value,
                              int *bitpos_len)
{
    long long value = 0;
    unsigned char *bo_start = NULL;
    *bitpos_len = 0;

    while ((**bo == ' ') && (*args_len > 0)) {
        (*bo)++; (*args_len)--;
    }

    if (*args_len == 0) {
        return EXOREDIS_ARGS_MISSING;
    }

    bo_start = *bo;
    /* We have a non-space character */
    while (isdigit(**bo) && (*args_len) > 0) {
        (*bo)++; (*args_len)--; (*bitpos_len)++;
    }

    if (*args_len != 0) {
        /* At this point only a space is expected */
        if (!isspace(**bo)) {
            /* Unexpected non-digit character */
            return EXOREDIS_BO_ARGS_INVALID;
        }
    }

    /* Convert value between bo_start and *bo to an integer */
    value = strtoll((char *)bo_start, (char **)bo, 10);

    if ((value < 0) || (value > UINT_MAX)) {
        /* Unexpected non-digit character */
        return EXOREDIS_BO_ARGS_INVALID;
    }
    *bo_value = (unsigned int)(value);

    return EXOREDIS_OK;
}


exoredis_return_codes
exoredis_parse_bit_arg (unsigned char **buf,
                        int *args_len,
                        unsigned char *bitset)
{
    while ((**buf == ' ') && (*args_len > 0)) {
        (*buf)++; (*args_len)--;
    }

    *bitset = (unsigned char)(atoi((char *)(*buf)));
    if (*bitset & ~1) {
        return EXOREDIS_BO_ARGS_INVALID;
    }
    return (*args_len == 0)? EXOREDIS_ARGS_MISSING : EXOREDIS_OK;

}

void exoredis_handle_set (unsigned char *key,
                          int args_len)
{
    unsigned char *ptr = NULL;
    int key_len = 0;
    int value_len = 0;

    /* Format : SET key string */
    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SET' command");
        return;
    }

    ptr = key + key_len;
    if (exoredis_parse_value_arg(&ptr, &args_len, &value_len) ==
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SET' command");
        return;
    }

    printf("SET Command: SET %s %d %s %d \n", key, key_len, ptr, value_len);
    /* Write the value into hash */
    exoredis_create_update_he(key, key_len, ptr, value_len);
    exoredis_resp_ok();
    return;
}

/*
 * exoredis_handle_get():
 * This function handles the REDIS protocol GET request
 * It looks up the DB file passed in as argument, and:
 * 1. If key exists, returns the value if the value is string. 
 * 2. If key exists, error is returned if value is not string
 * 3. If key does not exist, returns a bulk string nil.
 */

void exoredis_handle_get (unsigned char *key,
                          int args_len)
{
    int key_len = 0;
    int value_len = 0;
    unsigned char *value = NULL;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'GET' command");
        return;
    }

    value = key + key_len;
    if (exoredis_parse_value_arg(&value, &args_len, &value_len) ==
        EXOREDIS_OK) {
        /* Hmm we don't expect anything beyond the key */
        exoredis_resp_error("wrong number of arguments for 'GET' command");
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

void exoredis_handle_setbit (unsigned char *key,
                             int args_len)
{
    unsigned int bo = 0;
    int key_len = 0;
    int bitpos_len = 0;
    unsigned char bitset;
    exoredis_return_codes ret;
    unsigned char *bitpos;
    unsigned char *bitchar;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SETBIT' command");
        return;
    }

    printf("SETBIT Command: SETBIT %s (%d) \n", key, key_len);
    bitpos = key + key_len;
    if ((ret = exoredis_parse_bitoffset_arg(&bitpos, &args_len, &bo,
                                            &bitpos_len) != EXOREDIS_OK)) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'SETBIT' command");
        } else {
            exoredis_resp_error("bit offset is not a integer or out of range");
        }
        return;
    }

    printf("SETBIT Command: SETBIT %s (%d) %u \n", key, key_len, bo);
    bitchar = bitpos + bitpos_len;
    if ((ret = exoredis_parse_bit_arg(&bitchar, &args_len, &bitset) !=
        EXOREDIS_OK)) {
            if (ret == EXOREDIS_BO_ARGS_INVALID) {
                exoredis_resp_error("bit offset is not a integer or out of range");
            } else {
                exoredis_resp_error("wrong number of arguments for 'SETBIT' command");
            }
            return;
    }
    printf("SETBIT Command: SETBIT %s (%d) %u %c \n", key, key_len, bo, bitset);

    exoredis_resp_ok();
    return;
}
