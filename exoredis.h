/*
 * exoredis.h
 * This file includes all quintessential header files to be included.
 */

#ifndef _EXOREDIS_H_

#define _EXOREDIS_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <signal.h>

#define EXOREDIS_MAX_SERVER_CONN 5

#define EXOREDIS_SERVER_TCP_PORT 15000

#define MAX_REQ_RESP_MSGLEN 1024

typedef enum exoredis_data_type
{
  SIMPLE_STRING_DATA_TYPE,
  ERROR_DATA_TYPE,
  INTEGER_DATA_TYPE,
  BULK_STRING_DATA_TYPE,
  ARRAY_DATA_TYPE,
  MAX_DATA_TYPE,
}exoredis_data_type;

typedef enum {
    SIMPLE_STRING_OK,
    SIMPLE_STRING_MAX,
}exoredis_simple_strings;

typedef enum {
    ERROR_STRING_ERR,
    ERROR_STRING_MAX,
}exoredis_error_strings;

#define EXOREDIS_RESP_LEN 1024

typedef enum supported_cmds
{
    EXOREDIS_CMD_GET,
    EXOREDIS_CMD_SET,
    EXOREDIS_CMD_GETBIT,
    EXOREDIS_CMD_SETBIT,
    EXOREDIS_CMD_ZADD,
    EXOREDIS_CMD_ZCOUNT,
    EXOREDIS_CMD_ZCARD,
    EXOREDIS_CMD_ZRANGE,
    EXOREDIS_CMD_SAVE,
    EXOREDIS_CMD_MAX
}exoredis_supported_cmds;
#endif
