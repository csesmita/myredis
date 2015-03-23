#include "exordb.h"
#include "exoredis.h"
#include "exoredis_server.h"
/*
 * Frame Format :
 * 
 * Type - (LE Key + Key ) - (LE Value + Value)
 */

exoredis_return_codes
exordb_encode_type (char **buf,
                    exoredis_value_type type)
{
    memcpy((*buf), (char *)&type, 1);
    (*buf)++;
    return EXOREDIS_OK;
}

exoredis_value_type
exordb_decode_type (char **buf)
{
    exoredis_value_type val = **buf;
    (*buf)++;
    return val;
}

exoredis_return_codes
exordb_encode_key (char **buf,
                   char  *key)
{
    unsigned int key_len = strlen(key);
    unsigned char len_encode = EXOREDIS_RDB_6BITLEN;
    unsigned short short_key = 0;

    if (key_len > EXOREDIS_RDB_6BITLEN_LIMIT) {
        if (key_len > EXOREDIS_RDB_14BITLEN_LIMIT) {
            printf("Keys of lengths greater than 16KB not supported\n");
            return EXOREDIS_ERR;
        } else {
            len_encode = EXOREDIS_RDB_14BITLEN;
        }
    }

    /* Copy the two LSB of len_encode into buffer's MSBs 
     * Then copy in the length for the following bits/bytes
     */
    switch(len_encode) {
    case EXOREDIS_RDB_6BITLEN:
        (*buf)[0] = ((len_encode << 6) | ((unsigned char)(key_len) & 0x3F));
        (*buf)++;
        memcpy(*buf, key, key_len);
        break;
    case EXOREDIS_RDB_14BITLEN: 
        short_key = ((((unsigned short)(len_encode)) << 14) |
                      ((unsigned short)(key_len) & 0x3FFF));
        memcpy(*buf, &short_key, sizeof(short_key));
        *buf += sizeof(short_key);
        memcpy(*buf, key, key_len);
        break;
    default : /* Invalid */
        return EXOREDIS_ERR;
    }
    *buf += key_len;
    return EXOREDIS_ERR;
}

exoredis_return_codes
exordb_decode_key (char  *buf,
                   char **key)
{
    int key_len = 0;
    unsigned char len_encode = EXOREDIS_RDB_6BITLEN;
    char *ptr = buf;

    len_encode = (ptr[0] & 0xC0) >> 6;

    /* Copy the two LSB of len_encode into buffer's MSBs */
    switch(len_encode) {
    case EXOREDIS_RDB_6BITLEN:
        key_len = ptr[0] & 0x3F;
        ptr++;
        memcpy(*key, ptr, key_len);
        return EXOREDIS_OK;
    case EXOREDIS_RDB_14BITLEN: 
        key_len = (unsigned short)(*ptr) & 0x3FFF;
        ptr += sizeof(unsigned short);
        memcpy(*key, ptr, key_len);
        return EXOREDIS_OK;
    default : /* Invalid */
        break;
    }
    return EXOREDIS_ERR;
}
