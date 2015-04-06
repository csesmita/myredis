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
                    exoredis_value_type type,
                    unsigned int *total_len)
{
    memcpy((*buf), (char *)&type, 1);
    *total_len = *total_len + 1;
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
                   const char *key,
                   unsigned short key_len,
                   unsigned int *total_len)
{
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
        *total_len += 1;
        memcpy(*buf, key, key_len);
        break;
    case EXOREDIS_RDB_14BITLEN: 
        short_key = ((((unsigned short)(len_encode)) << 14) |
                      ((unsigned short)(key_len) & 0x3FFF));
        memcpy(*buf, &short_key, sizeof(short_key));
        *total_len += sizeof(short_key);
        *buf += sizeof(short_key);
        memcpy(*buf, key, key_len);
        break;
    default : /* Invalid */
        return EXOREDIS_ERR;
    }
    *total_len += key_len;
    *buf += key_len;
    return EXOREDIS_ERR;
}

exoredis_return_codes
exordb_decode_key (char **buf,
                   char **key)
{
    unsigned short key_len = 0;
    unsigned char len_encode = EXOREDIS_RDB_6BITLEN;

    len_encode = (**buf & 0xC0) >> 6;

    /* Copy the two LSB of len_encode into buffer's MSBs */
    switch(len_encode) {
    case EXOREDIS_RDB_6BITLEN:
        key_len = **buf & 0x3F;
        (*buf)++;
        memcpy(*key, *buf, key_len);
        *buf += key_len;
        return EXOREDIS_OK;
    case EXOREDIS_RDB_14BITLEN: 
        memcpy(&key_len, (unsigned short *)(*buf), sizeof(unsigned short));
        key_len &= 0x3FFF;
        *buf += sizeof(unsigned short);
        memcpy(*key, *buf, key_len);
        return EXOREDIS_OK;
    default : /* Invalid */
        break;
    }
    return EXOREDIS_ERR;
}

exoredis_return_codes
exordb_encode_value (char **buf,
                     const char  *value,
                     unsigned short val_len,
                     exoredis_value_type val_type,
                     exoredis_data_type  data_type,
                     unsigned short size,
                     unsigned int *total_len)
{
    unsigned char len_encode = EXOREDIS_RDB_6BITLEN;
    unsigned short short_key = 0;

    if (val_len > EXOREDIS_RDB_14BITLEN_LIMIT) {
        printf("Value of the key cannot exceed 16KB\n");
        return EXOREDIS_ERR;
    }

    len_encode = (val_len > EXOREDIS_RDB_6BITLEN_LIMIT)? EXOREDIS_RDB_14BITLEN:
                 EXOREDIS_RDB_6BITLEN;

    switch(val_type) {
        case ENCODING_VALUE_TYPE_STRING:
            /* Check the specific data type when encoding the length of value*/
            switch(data_type) {
                case SIMPLE_STRING_DATA_TYPE:
                case ERROR_DATA_TYPE:
                case BULK_STRING_DATA_TYPE:
                    if (len_encode == EXOREDIS_RDB_6BITLEN) {
                        (*buf)[0] = (len_encode << 6) | (val_len & 0x3F); 
                        (*buf)++;
                        *total_len += 1;
                    } else {
                        short_key = (unsigned short)(len_encode << 6) |
                                    (val_len & 0x3FFF); 
                        memcpy(*buf, &short_key, sizeof(short_key));
                        (*buf) += sizeof(short_key);
                        *total_len += sizeof(short_key);
                    }
                    memcpy(*buf, value, val_len);
                    *buf += val_len;
                    *total_len += val_len;
                    break;

                case INTEGER_DATA_TYPE:
                    len_encode = EXOREDIS_RDB_SPECIAL_BITLEN;
                    /* Integer length have to be contained within 4 bits */
                    if (val_len > EXOREDIS_RDB_SPECIAL_FOUR_BYTE) {
                        printf("Byte length of integer value cannot exceed 4 bytes\n");
                        return EXOREDIS_ERR;
                    }
                    (*buf)[0] = (len_encode << 6) | ((unsigned char)(val_len) & 0x3F); 
                    (*buf)++;
                    *total_len += 1;
                    memcpy(*buf, value, val_len);
                    *buf += val_len;
                    *total_len += val_len;
                    break;
                default:
                    return EXOREDIS_ERR;
            }
            return EXOREDIS_OK;

        case ENCODING_VALUE_TYPE_LIST:
        case ENCODING_VALUE_TYPE_SORTED_SET:
            if (size > EXOREDIS_RDB_14BITLEN_LIMIT) {
                printf("size of list cannot exceed 16383 in count\n");
                return EXOREDIS_ERR;
            }
            switch(data_type) {
                case ARRAY_DATA_TYPE:
                    /* First encode the size of array, followed by
                     * len + value of each element 
                     */
                    len_encode = (size > EXOREDIS_RDB_6BITLEN_LIMIT)? 
                                 EXOREDIS_RDB_14BITLEN : EXOREDIS_RDB_6BITLEN;
                    if(len_encode == EXOREDIS_RDB_6BITLEN) {
                        (*buf)[0] = (len_encode << 6) | ((unsigned char)(size) & 0x3F); 
                        (*buf)++;
                        *total_len += 1;
                    } else {
                        short_key = (unsigned short)(len_encode << 6) | (size & 0x3FFF); 
                        memcpy(*buf, &short_key, sizeof(short_key));
                        (*buf) += sizeof(short_key);
                        *total_len += sizeof(short_key);
                    }

                    /* Simple encoded strings in value. Copy in */
                    memcpy(*buf, value, val_len);
                    (*buf) += val_len;
                    *total_len += val_len;
                    break;
                default:
                    return EXOREDIS_ERR;
            }
            return EXOREDIS_OK;

        default:
            break;
    }
    return EXOREDIS_ERR;
}

exoredis_return_codes
exordb_decode_value (char  *buf,
                     char **value,
                     unsigned short *val_len,
                     exoredis_value_type  val_type,
                     exoredis_data_type  *data_type,
                     unsigned short *size)
{
    unsigned char len_encode = EXOREDIS_RDB_6BITLEN;
    unsigned short short_key = 0;

    switch(val_type) {
        case ENCODING_VALUE_TYPE_STRING:
            /* Check the specific data type when encoding the length of value */
            len_encode = (buf[0] >> 6 ) & 0x03;
            switch(len_encode) {
                case EXOREDIS_RDB_6BITLEN:
                    /* Read the 6 bits of buf */
                    *val_len = buf[0] & 0x3F;
                    buf++;
                    
                    /* Read this length value into value buffer */
                    memcpy(*value, buf, *val_len);
                    *data_type = BULK_STRING_DATA_TYPE;
                    break;

                case EXOREDIS_RDB_14BITLEN:
                    /* Read the 14 bits of buf */
                    short_key = 0;
                    memcpy(&short_key, buf, sizeof(short_key));
                    *val_len = short_key & 0x3FFF;

                    buf += sizeof(short_key);

                    /* Read this length value into value buffer */
                    memcpy(*value, buf, *val_len);
                    *data_type = BULK_STRING_DATA_TYPE;
                    
                    break;
                case EXOREDIS_RDB_SPECIAL_BITLEN:
                    /* Read the 6 bits of buf */
                    *val_len = buf[0] & 0x3F;
                    buf++;
                    
                    /* Integer length have to be contained within 4 bits */
                    if (*val_len > EXOREDIS_RDB_SPECIAL_FOUR_BYTE) {
                        printf("Byte length of integer value cannot exceed 4 bytes\n");
                        return EXOREDIS_ERR;
                    }
                    /* Read this length value into value buffer */
                    memcpy(*value, buf, *val_len);
                    *data_type = INTEGER_DATA_TYPE;
                    break;

                default:
                    return EXOREDIS_ERR;
            }
            return EXOREDIS_OK;

        case ENCODING_VALUE_TYPE_LIST:
        case ENCODING_VALUE_TYPE_SORTED_SET:
            /* Decode the size using LE */
            *data_type = ARRAY_DATA_TYPE;
            len_encode = (buf[0] >> 6 ) & 0x03;
            switch(len_encode) {
                case EXOREDIS_RDB_6BITLEN:
                    /* Read the 6 bits of buf */
                    *size = buf[0] & 0x3F;
                    buf++;
                    break;

                case EXOREDIS_RDB_14BITLEN:
                    /* Read the 14 bits of buf */
                    short_key = 0;
                    memcpy(&short_key, buf, sizeof(short_key));
                    *size = short_key & 0x3FFF;

                    buf += sizeof(short_key);
                    break;

                default:
                    return EXOREDIS_ERR;
            }
            if (*size > EXOREDIS_RDB_14BITLEN_LIMIT) {
                printf("size of list cannot exceed 16383 in count\n");
                return EXOREDIS_ERR;
            }
            return EXOREDIS_OK;

        default:
            break;
    }
    return EXOREDIS_ERR;
}


#ifdef TEST_MODE_ON

void main()
{
    char buf[2048];
    char key[2048];
    char *key_ptr = NULL;
    char *eptr = NULL;
    char *dptr = NULL;
    unsigned int total_len = 0, i = 0;
    exoredis_value_type dval;
    unsigned short val_len, size;
    exoredis_data_type data_type;

    memset(buf, 0 , 2048);
    eptr = dptr = buf;

    exordb_encode_type(&eptr, ENCODING_VALUE_TYPE_STRING, &total_len);
    exordb_encode_key(&eptr, "somekey", strlen("somekey"), &total_len);
    exordb_encode_value(&eptr, "somekeyvalue", strlen("somekeyvalue"),
        ENCODING_VALUE_TYPE_STRING, BULK_STRING_DATA_TYPE, 0, &total_len);

    printf("Length of encoded buffer %u\nBuffer:\n", total_len);
    while (i < total_len) {
        printf("%x",buf[i++]);
    }
    printf("\nTrying to decode \n");

    dval = exordb_decode_type(&dptr);
    printf("Type: %s\n", (dval == ENCODING_VALUE_TYPE_STRING)? "Correct":"Wrong");

    key_ptr = key;
    exordb_decode_key(&dptr, &key_ptr);
    printf("Key: %s\n", key);

    key_ptr = key;
    exordb_decode_value(dptr, &key_ptr, &val_len, dval, &data_type, &size);
    printf("Value: %s\n", key);
}
#endif
