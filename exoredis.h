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
#define MAX_REQ_RESP_INT_NUM (MAX_REQ_RESP_MSGLEN/sizeof(int))

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
    ERROR_STRING_WRONGTYPE,
    ERROR_STRING_MAX,
}exoredis_error_strings;


typedef enum supported_cmds
{
    EXOREDIS_CMD_GET = 1,
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

#define EXOREDIS_LITTLE_ENDIAN_BIT_POS(x) (7 - (x))

typedef enum _exoredis_value_type {
    ENCODING_VALUE_TYPE_STRING,
    ENCODING_VALUE_TYPE_STRING_SEC_EX,
    ENCODING_VALUE_TYPE_STRING_SEC_PX,
    ENCODING_VALUE_TYPE_STRING_SEC_EX_PX,
    ENCODING_VALUE_TYPE_SORTED_SET,
    ENCODING_VALUE_TYPE_MAX,
}exoredis_value_type;

typedef enum _set_options {
    OPTION_EX = 1,
    OPTION_PX,
    OPTION_EX_PX,
    OPTION_NX,
    OPTION_XX,
    OPTION_MAX,
}exoredis_set_options;

/* Similar to .rdb definition, this field specifies the length of the length 
 * field. The most significant two bits of the first byte of length specifies
 * the format of the length field. E.g.: 
 * 00|000000: Length field is the 6 bits following the 2 MSBs
 * 01|000000: Length field is 14 bits following the 2 MSBs
 * 10|000000: Invalid. Not using the 32-bit encapsulation
 * 11|000000: Length field is a special integer value.
 *            Read the next 2 bits to obtain length of integer
 */
#define EXOREDIS_RDB_6BITLEN  0
#define EXOREDIS_RDB_14BITLEN 1
/* 32 bit lengths not supported */
#define EXOREDIS_RDB_SPECIAL_BITLEN 3

#define EXOREDIS_RDB_SPECIAL_ONE_BYTE 0
#define EXOREDIS_RDB_SPECIAL_TWO_BYTE 1
#define EXOREDIS_RDB_SPECIAL_FOUR_BYTE 2

#define EXOREDIS_RDB_6BITLEN_LIMIT 63
#define EXOREDIS_RDB_14BITLEN_LIMIT 16383

/* The pattern inside the content bits ends up looking as:
 * Value Type - Key - Value,
 * where the value type determines the encoding in value.
 *
 * String Encoding(type ENCODING_VALUE_TYPE_STRING): 
 *                  00/01 for Simple, Bulk, Error
 *                  11 for Integers
 *
 * Sorted Set(type ENCODING_VALUE_TYPE_SORTED_SET):
 * Number of elements, size, from length encoding like in strings
 * For number of strings in len, string interpretation
 *
 * For each element, encoding is:
 * len | value | len | score
 *
 */

#define UINT_MAX 4294967295U 

/* Status codes for encoding/decoding */
typedef enum {
    EXOREDIS_OK,
    EXOREDIS_ERR,
    EXOREDIS_ARGS_MISSING,
    EXOREDIS_BO_ARGS_INVALID,
    EXOREDIS_WRONGTYPE,
} exoredis_return_codes;

#define PATH_MAX 512
struct _exoredis_io {
    char filePath[PATH_MAX];
    /* Write to/read from client socket */
    int fd;
};

typedef struct _exoredis_io exoredis_io_name;

/* The IO specifics' variable */
exoredis_io_name exoredis_io;

//#define TEST_MODE_ON

void exoredis_resp_bulk_string (unsigned char *msg,
                                int len,
                                unsigned char to_send,
                                unsigned char **buf,
                                int *buf_len);
#endif
