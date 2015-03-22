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

    buf[0] = prefix_string[ERROR_DATA_TYPE];
    strncat(buf, error_string[ERROR_STRING_ERR], 
            strlen(error_string[ERROR_STRING_ERR]));
    strncat(buf, msg, strlen(msg));
    send(exoredis_io.fd, buf, strlen(buf), MSG_DONTWAIT);
}

void exoredis_handle_set (char *key)
{
    char *ptr = NULL;

    /* Format : SET key string */
    ptr = key;
    while (*++ptr != ' ');
    *ptr++ = '\0';
    if (!strlen(key) || !strlen(ptr)) {
        return exoredis_resp_error("Incorrect arguments to SET\n");
    }
    printf("SET Command: SET %s %s\n", key, ptr);
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
