/*
 * exordb.h:
 * This file defines the file format of the .exordb DB file
 * Note: No Magic Number, Version Number and DB selector fields are used in
 * .exordb, as in .rdb
 * Encoder Decoder definitions follow
 */

#ifndef _EXORDB_H
#define _EXORDB_H

#include <stdio.h>
/* Reference code from redis-2.8.19/src/config.h */
#include <sys/types.h>

#ifndef BYTE_ORDER
#if (BSD >= 199103)
# include <machine/endian.h>
#else
#if defined(linux) || defined(__linux__)
# include <endian.h>
#else
#define	LITTLE_ENDIAN	1234	/* least-significant byte first (vax, pc) */
#define	BIG_ENDIAN	4321	/* most-significant byte first (IBM, net) */
#define	PDP_ENDIAN	3412	/* LSB first in word, MSW first in long (pdp)*/

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || \
   defined(vax) || defined(ns32000) || defined(sun386) || \
   defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || \
   defined(__alpha__) || defined(__alpha)
#define BYTE_ORDER    LITTLE_ENDIAN
#endif

#if defined(sel) || defined(pyr) || defined(mc68000) || defined(sparc) || \
    defined(is68k) || defined(tahoe) || defined(ibm032) || defined(ibm370) || \
    defined(MIPSEB) || defined(_MIPSEB) || defined(_IBMR2) || defined(DGUX) ||\
    defined(apollo) || defined(__convex__) || defined(_CRAY) || \
    defined(__hppa) || defined(__hp9000) || \
    defined(__hp9000s300) || defined(__hp9000s700) || \
    defined (BIT_ZERO_ON_LEFT) || defined(m68k) || defined(__sparc)
#define BYTE_ORDER	BIG_ENDIAN
#endif
#endif /* linux */
#endif /* BSD */
#endif /* BYTE_ORDER */

/* Sometimes after including an OS-specific header that defines the
 * endianess we end with __BYTE_ORDER but not with BYTE_ORDER that is what
 * the Redis code uses. In this case let's define everything without the
 * underscores. */
#ifndef BYTE_ORDER
#ifdef __BYTE_ORDER
#if defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif
#endif
#endif

/* End Reference Code */

#if !defined(BYTE_ORDER) || \
    (BYTE_ORDER != BIG_ENDIAN && BYTE_ORDER != LITTLE_ENDIAN)
unsigned int byte_test_integer = 0xabcdefff;
#define BYTE_ORDER (byte_test_integer & 0x00000011 == 0x000000ff)? \
                    BIG_ENDIAN : LITTLE_ENDIAN
#endif

#define EXOREDIS_LITTLE_ENDIAN_BIT_POS(x) (7 - (x))

typedef enum _exoredis_value_type {
    ENCODING_VALUE_TYPE_STRING,
    ENCODING_VALUE_TYPE_STRING_SEC_EX,
    ENCODING_VALUE_TYPE_STRING_MSEC_EX,
    ENCODING_VALUE_TYPE_SORTED_SET,
    ENCODING_VALUE_TYPE_MAX,
}exoredis_value_type;


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
    /* DB file pointer */
    FILE *dbfp;
    char filePath[PATH_MAX];
    /* Write to/read from client socket */
    int fd;
};

typedef struct _exoredis_io exoredis_io_name;

/* The IO specifics' variable */
exoredis_io_name exoredis_io;

//#define TEST_MODE_ON
#endif /* _EXORDB_H */
