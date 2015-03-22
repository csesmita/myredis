/*
 * exordb.h:
 * This file defines the file format of the .exordb DB file
 * Note: No Magic Number, Version Number and DB selector fields are used in
 * .exordb, as in .rdb
 */

#ifndef _EXORDB_H
#define _EXORDB_H
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
 * Type - Key - Value
 */


struct _exoredis_io {
    union {
        struct {
            FILE *fp;
        }file;
        
        struct {
            char *buf;
            int *offset;
        }buffer;
    }storage;
    /* Write to/read from client socket */
    int fd;
};

typedef struct _exoredis_io exoredis_io_name;

exoredis_io_name exoredis_io;
#endif /* _EXORDB_H */
