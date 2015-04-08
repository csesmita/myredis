#include "exoredis.h"
#include "exoredis_server.h"
#include "exoredis_hash.h"
#include "exordb.h"

/* Define prefix strings for the data types */
char prefix_string[MAX_DATA_TYPE] = {'+','-',':','$','*'};

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

void exoredis_resp_error(char *msg)
{
    char buf[EXOREDIS_RESP_LEN] = "\0";

    /* OK  to use string operations since these are always simple strings */
    buf[0] = prefix_string[ERROR_DATA_TYPE];
    strncat(buf, error_string[ERROR_STRING_ERR], 
            strlen(error_string[ERROR_STRING_ERR]));
    strncat(buf, msg, strlen(msg));
    send(exoredis_io.fd, buf, strlen(buf), MSG_DONTWAIT);
}

void exoredis_resp_ok(char *msg)
{
    char buf[EXOREDIS_RESP_LEN] = "\0";

    /* OK  to use string operations since these are always simple strings */
    buf[0] = prefix_string[SIMPLE_STRING_DATA_TYPE];
    strncat(buf, simple_string[SIMPLE_STRING_OK], 
            strlen(simple_string[SIMPLE_STRING_OK]));
    strncat(buf, msg, strlen(msg));
    send(exoredis_io.fd, buf, strlen(buf), MSG_DONTWAIT);
}

void exoredis_handle_set (unsigned char *key,
                          int args_len)
{
    unsigned char *ptr = NULL;
    int key_len = 0;
    int value_len = 0;

    /* Format : SET key string */
    ptr = key;

    /* Check for key value format */
    while ((*ptr != ' ') && (key_len < args_len)) {
        ptr++; key_len++;
    }
    printf("key_len %d ptr %s\n", key_len, ptr);
    value_len = args_len - key_len;
    while ((*ptr == ' ') && (value_len > 0)) {
        ptr++; value_len--;
    }

    if (!key_len || !value_len) {
        return exoredis_resp_error("Incorrect arguments to SET\n");
    }
    printf("SET Command: SET %s %s\n", key, ptr);
    printf("SET Command: Length of args %d %d\n", key_len, value_len);
    /* Write the value into hash */
    exoredis_create_update_he(key, key_len, ptr, value_len);
    return;
}

void exoredis_handle_save(char *arg)
{
    printf("SAVE Command: %s\n", "+OK");
    
    /* Dump the contents of the hash table into the DB File */
    exoredis_feed_ht_to_io();

    /* For all other entries that exist, look for duplicates and delete */
    
    return;
}
