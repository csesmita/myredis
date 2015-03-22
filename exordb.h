/*
 * exordb.h:
 * This file defines the file format of the .exordb DB file
 * Note: No Magic Number, Version Number and DB selector fields are used in
 * .exordb, as in .rdb
 * Encoder Decoder definitions follow
 */

#ifndef _EXORDB_H
#define _EXORDB_H

typedef enum _exoredis_value_type {
    ENCODING_VALUE_TYPE_STRING,
    ENCODING_VALUE_TYPE_LIST,
    ENCODING_VALUE_TYPE_SORTED_SET,
}exoredis_value_type;


/* Similar to .rdb definition, this field specifies the length of the length 
 * field. The most significant two bits of the first byte of length specifies
 * the format of the length field. E.g.: 
 * 00|000000: Length field is the 6 bits following the 2 MSBs
 * 01|000000: Length field is 14 bits following the 2 MSBs
 * 10|000000: Length field is a 32-bit field following the 2 MSBs
 * 11|000000: Invalid. Not using the special encapsulation
 */
#define EXOREDIS_RDB_6BITLEN  0
#define EXOREDIS_RDB_14BITLEN 1
#define EXOREDIS_RDB_32BITLEN 2

/* The pattern inside the content bits ends up looking as:
 * Value Type - Key - Value,
 * where the value type determines the encoding in value.
 *
 * String Encoding(type ENCODING_VALUE_TYPE_STRING): 
 *                  00/01/10 for Simple, Bulk, Error
 *                  11 for Integers
 *
 * List Encoding(type ENCODING_VALUE_TYPE_LIST): 
 * Number of elements, size, from length encoding like in strings
 * For number of strings in len, string interpretation
 *
 * Sorted Set(type ENCODING_VALUE_TYPE_SORTED_SET):
 * Like in List encoding, only for each element, encoding is:
 * len | value | len | score
 *
 */

struct _exoredis_io {
    /* DB file pointer */
    FILE *dbfp;

    /* Write to/read from client socket */
    int fd;
};

typedef struct _exoredis_io exoredis_io_name;

/* The IO specifics' variable */
exoredis_io_name exoredis_io;

#endif /* _EXORDB_H */
