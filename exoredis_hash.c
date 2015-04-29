/*
 * exoredis_hash.c:
 * Implements the hash utilities
 */

#include "exoredis.h"
#include "exoredis_server.h"
#include "exordb.h"
#include "exoredis_hash.h"
#include <limits.h>

exoredis_ht *ht = NULL;

void
exoredis_create_ht (exoredis_ht **hash_t,
                    unsigned size)
{
    int i = 0;
    *hash_t = (exoredis_ht *) malloc(sizeof(exoredis_ht));
    (*hash_t)->size = size;
    (*hash_t)->table = (exoredis_hash_entry **) malloc(size * sizeof(exoredis_hash_entry *));
    while (i < size) {
        (*hash_t)->table[i++] = NULL;
    }
}

void
exoredis_destroy_ht (exoredis_ht **hash_t)
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    if (!hash_t || !(*hash_t)) {
        return;
    }

    while (i < (*hash_t)->size) {
        while((*hash_t)->table[i]) {
            temp = (*hash_t)->table[i];
            (*hash_t)->table[i] = (*hash_t)->table[i]->next;
            free(temp);
        }
        i++; 
    }

    free(*hash_t);
    *hash_t = NULL;
}

void
exoredis_dump_ht (exoredis_ht **hash_t)
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    if (!hash_t || !(*hash_t)) {
        return;
    }

    while (i < (*hash_t)->size) {
        temp = (*hash_t)->table[i];
        while (temp) {
            printf("Hash %d:Lengths %d %d\n", i, temp->key_len, temp->value_len);
            printf("Hash %d:Got key value pair (%s,%s)\n", i, temp->key, temp->value);
            temp = temp->next;
        }
        i++;
    }
}

unsigned int
exoredis_hash_index (exoredis_ht **hash_t,
                     unsigned char *key,
                     int key_len)
{
    unsigned int hash_value = EXOREDIS_HASH_SEED;
    unsigned int i = 0;
    unsigned char *temp_key = key;

    while (hash_value < ULONG_MAX && i < key_len) {
        hash_value = hash_value << 8;
        hash_value += temp_key[i++];
    }
    /*
    printf("Hash index for key %s is %u\n", key, 
           (unsigned int)(hash_value % (*hash_t)->size));
    */
    return (unsigned int)(hash_value % (*hash_t)->size);
}

void
exoredis_destroy_he (unsigned char *key,
                     int key_len)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index(&ht, key, key_len);

    he_temp = ht->table[ht_index];
    he_prev = NULL;

    /* First check for the key/value pair */
    while(he_temp && memcmp(he_temp->key, key, key_len)) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Destroy it */
        if (he_temp == ht->table[ht_index]) {
            ht->table[ht_index] = ht->table[ht_index]->next;
        } else if (he_prev) {
            he_prev->next = he_temp->next;
        }
        free(he_temp);
    } else {
        /* Node doesn't exist */
        printf("Key %s not found in Update Hash\n", key);
    }
}

exoredis_hash_entry *
exoredis_lookup_create_update_he (unsigned char *key, 
                                  int key_len,
                                  unsigned char *value,
                                  int value_len,
                                  int flags,
                                  int *expires,
                                  exoredis_value_type type)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    exoredis_hash_entry *he_next = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index(&ht, key, key_len);
    he_temp = ht->table[ht_index];

    he_prev = NULL;

    /* First check for the key/value pair */
    while(he_temp && memcmp(he_temp->key,key, key_len)) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        if(flags & OPTION_NX) {
            /* Only update if key doesn't exist */
            return NULL;
        }

        /* Node exists. Update the value */
        he_next = he_temp->next;
        he_temp = (exoredis_hash_entry *)realloc(he_temp, 
                   sizeof(exoredis_hash_entry) + value_len);
        memset(he_temp, 0, sizeof(exoredis_hash_entry) + value_len);
        memcpy(he_temp->value, value, value_len);
        memcpy(he_temp->key, key, key_len);
        he_temp->key_len = key_len;
        he_temp->value_len = value_len;
        he_temp->next = he_next;
        he_temp->type = type;
        if (expires) {
            he_temp->expires_value[0] = expires[0]; 
            he_temp->expires_value[1] = expires[1]; 
        }

        if (he_prev) {
            he_temp->next = he_prev->next;
            he_prev->next = he_temp;
        } else {
            ht->table[ht_index] = he_temp;
        }
    } else {
        if(flags & OPTION_XX) {
            /* Only update if key exists */
            return NULL;
        }

        he_temp = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry)
                                                + value_len);
        memset(he_temp, 0, sizeof(exoredis_hash_entry) + value_len);
        he_temp->next = NULL;
        memcpy(he_temp->key, key, key_len);
        memcpy(he_temp->value, value, value_len);
        he_temp->key_len = key_len;
        he_temp->value_len = value_len;
        he_temp->type = type;
        if (expires) {
            he_temp->expires_value[0] = expires[0]; 
            he_temp->expires_value[1] = expires[1];
        }

        if (he_prev) {
            he_temp->next = he_prev->next;
            he_prev->next = he_temp;
        } else {
            ht->table[ht_index] = he_temp;
        }
    }
    printf("Dumping hash tree \n");
    exoredis_dump_ht(&ht);

    return he_temp;
}

int
exoredis_string_type_mismatch (exoredis_value_type type)
{
    if ((type != ENCODING_VALUE_TYPE_STRING) &&
        (type != ENCODING_VALUE_TYPE_STRING_SEC_EX) &&
        (type != ENCODING_VALUE_TYPE_STRING_SEC_PX) &&
        (type != ENCODING_VALUE_TYPE_STRING_SEC_EX_PX)) {
        return 1;
    }

    return 0;
}

void *
exoredis_read_he (unsigned char *key,
                  int key_len,
                  unsigned char **value,
                  int *value_len,
                  exoredis_value_type *type)
{
    exoredis_hash_entry *he_temp = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index(&ht, key, key_len);
    he_temp = ht->table[ht_index];

    /* First check for the key/value pair */
    while(he_temp && memcmp(he_temp->key, key, key_len)) {
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Return the value */
        printf("Returning key value pair (%s,%s)\n",he_temp->key, he_temp->value);
        /* Onus of deallocation on the calling function */
        if (value && value_len && type) {
            *value = (unsigned char *)malloc(he_temp->value_len);
            memcpy(*value, he_temp->value, he_temp->value_len);
            *value_len = he_temp->value_len;
            *type = he_temp->type;
        }
        return he_temp;
    }

    printf("Invalid Key\n");
    return NULL;
}
exoredis_hash_entry *
exoredis_hash_get_next_node (exoredis_hash_entry *node,
                             int *index)
{
    exoredis_hash_entry *temp = NULL;

    if (!node) {
        return NULL;
    }

    if (node->next) {
        return node->next;
    }

    (*index)++;
    temp = NULL;
    while((!temp) && (*index < ht->size)) {
        temp = ht->table[(*index)];
        (*index)++;
    }
    if (*index == ht->size) {
        return NULL;
    }
    (*index)--;
    return temp;
}

exoredis_hash_entry *
exoredis_hash_get_first_node (int *index)
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    while ((i < ht->size) && !temp) {
        temp = ht->table[i];
        i++;
    }
  
    *index = i - 1;

    return (i == ht->size)? NULL : temp;
}

/* Dump the entries in the HT to the DB file */
exoredis_return_codes
exoredis_feed_ht_to_io (void)
{
    FILE *fp = NULL;
    exoredis_hash_entry *he_node = NULL;
    int index = 0;
    int card;
    unsigned char buf[MAX_REQ_RESP_MSGLEN];
    unsigned char *ptr = buf;
    int buf_len = 0, last = 0;

    /* Open a new file */
    if((fp = fopen(exoredis_io.filePath, "ab")) == NULL) {
        return EXOREDIS_ERR;
    }

    /*                  --- FORMAT ---                               */ 
    /*
     * TYPE = ENCODING_VALUE_TYPE_STRING
     * 
     * type | key_len |   key     |  value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) |   (4)      | (value_len) |
     *
     *
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_EX
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_PX
     * TYPE = ENCODING_VALUE_TYPE_SORTED_SET
     * 
     * type | key_len |   key     |  special | value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) |    (4)   |    (4)    | (value_len) |
     *
     * Special field indicates respectively:
     * Seconds to expiry
     * Milliseconds to Expiry
     * Size of Sorted Array
     *
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_EX_PX
     *
     * type | key_len |   key     |   ex  |  px | value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) | (4)   | (4) |    (4)    | (value_len) |
     *
     */

   
    /* Lookup hash and write to file */
    he_node = exoredis_hash_get_first_node(&index);
    while(he_node) {
        fwrite(&he_node->type, sizeof(he_node->type), 1, fp);
        fwrite(&he_node->key_len, sizeof(he_node->key_len), 1, fp);
        fwrite(he_node->key, 1, he_node->key_len, fp);

        /* Any expiry/size info to be written ? */
        switch(he_node->type) {
            case ENCODING_VALUE_TYPE_STRING_SEC_EX:
                fwrite(&he_node->expires_value[0], 
                       sizeof(he_node->expires_value[0]), 1, fp);
                break;
                
            case ENCODING_VALUE_TYPE_STRING_SEC_PX:
                fwrite(&he_node->expires_value[1], 
                       sizeof(he_node->expires_value[1]), 1, fp);
                break;
                
            case ENCODING_VALUE_TYPE_STRING_SEC_EX_PX:
                fwrite(&he_node->expires_value[0], 
                       sizeof(he_node->expires_value[0]), 1, fp);
                fwrite(&he_node->expires_value[1], 
                       sizeof(he_node->expires_value[1]), 1, fp);
                break;
                
            case ENCODING_VALUE_TYPE_SORTED_SET:
                exoredis_card_sortedset(he_node->key, he_node->key_len, &card);
                fwrite(&card, sizeof(card), 1, fp);
                break;

            default:
                break;
        }

        /* Write the value */
        switch(he_node->type) {
            case ENCODING_VALUE_TYPE_STRING:
            case ENCODING_VALUE_TYPE_STRING_SEC_EX:
            case ENCODING_VALUE_TYPE_STRING_SEC_PX:
            case ENCODING_VALUE_TYPE_STRING_SEC_EX_PX:
                fwrite(&he_node->value_len, sizeof(he_node->value_len), 1, fp);
                fwrite(&he_node->value, 1, he_node->value_len, fp);
                break;

            case ENCODING_VALUE_TYPE_SORTED_SET:
                /* Format of Sorted Set for Value field:
                 * 
                 * Size | Val_len |   Value   | Score | Val_len | ...
                 *
                 * (4)  |  (4)    | (Val_len) |  (4)  |  (4)    | ...
                 */

                last = card - 1;
                ptr = buf;
                exoredis_range_sortedset(he_node->key, he_node->key_len, 0,
                                         last, 1, &ptr, &buf_len, &card,
                                         TRUE);
                fwrite(buf, 1, buf_len, fp);
                break;
            
            default:
                break;
        }
        he_node = exoredis_hash_get_next_node(he_node,&index);
    }
    fflush(fp);
    fclose(fp);

    return (EXOREDIS_OK);
}


/* Load the entries in the DB file to the HT */
exoredis_return_codes
exoredis_feed_io_to_ht (void)
{
    FILE *fp = NULL;
    exoredis_value_type type;
    int key_len = 0;
    unsigned char key[EXOREDIS_HASH_KEY_LEN];
    int expires_value[2];
    int card, index= 0;
    unsigned int added = 0;
    unsigned char value[MAX_REQ_RESP_MSGLEN];
    unsigned char *ptr = value;
    int value_len_ss[MAX_REQ_RESP_MSGLEN];
    int value_len;
    int score[MAX_REQ_RESP_MSGLEN];

    /* Open the file in read-only binary mode */
    if((fp = fopen(exoredis_io.filePath, "rb")) == NULL) {
        return EXOREDIS_ERR;
    }

    /*                  --- FORMAT ---                               */ 
    /*
     * TYPE = ENCODING_VALUE_TYPE_STRING
     * 
     * type | key_len |   key     |  value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) |   (4)      | (value_len) |
     *
     *
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_EX
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_PX
     * TYPE = ENCODING_VALUE_TYPE_SORTED_SET
     *
     * type | key_len |   key     |  special | value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) |    (4)   |    (4)    | (value_len) |
     *
     * Special field indicates respectively:
     * Seconds to expiry
     * Milliseconds to Expiry
     * Size of Sorted Array
     *
     * TYPE = ENCODING_VALUE_TYPE_STRING_SEC_EX_PX
     *
     * type | key_len |   key     |   ex  |  px | value_len |    value    |
     *
     * (1)  |  (4)    | (key_len) | (4)   | (4) |    (4)    | (value_len) |
     *
     */

    /* Read the file according to the format */
    while(fread(&type, sizeof(exoredis_value_type), 1, fp) != 0) {
        fread(&key_len, sizeof(key_len), 1, fp);
        fread(key, 1, key_len, fp);

        /* Any expiry/size info to be read ? */
        card = 0; expires_value[0] = 0; expires_value[1] = 0;
        switch(type) {
            case ENCODING_VALUE_TYPE_STRING_SEC_EX:
                fread(&expires_value[0], sizeof(expires_value[0]), 1, fp);
                break;

            case ENCODING_VALUE_TYPE_STRING_SEC_PX:
                fread(&expires_value[1], sizeof(expires_value[1]), 1, fp);
                break;
                
            case ENCODING_VALUE_TYPE_STRING_SEC_EX_PX:
                fread(&expires_value[0], sizeof(expires_value[0]), 1, fp);
                fread(&expires_value[1], sizeof(expires_value[1]), 1, fp);
                break;
                
            case ENCODING_VALUE_TYPE_SORTED_SET:
                fread(&card, sizeof(card), 1, fp);
                break;
            default:
                break;
        }

        /* Read values */
        ptr = value;
        switch(type) {
            case ENCODING_VALUE_TYPE_STRING:
            case ENCODING_VALUE_TYPE_STRING_SEC_EX:
            case ENCODING_VALUE_TYPE_STRING_SEC_PX:
                fread(&value_len, sizeof(value_len), 1, fp);
                fread(&value, 1, value_len, fp);
                exoredis_lookup_create_update_he(key, key_len, value,
                                                 value_len, 0, expires_value,
                                                 type);
                break;
            case ENCODING_VALUE_TYPE_SORTED_SET:
                /* Format of Sorted Set for Value field:
                 * 
                 * Size | Val_len |   Value   | Score | Val_len | ...
                 *
                 * (4)  |  (4)    | (Val_len) |  (4)  |  (4)    | ...
                 */

                for (index = 0; index < card; index++) {
                    /* Form value_len, value arrays */
                    fread(&value_len_ss[index], sizeof(int), 1, fp);
                    fread(ptr, value_len_ss[index], 1, fp);
                    ptr += value_len_ss[index];
                    fread(&score[index], sizeof(int), 1, fp);
                }
                ptr = value;
                exoredis_add_sortedset(key, key_len, score, &ptr, value_len_ss,
                                       card, &added);
                break;
            default:
                break;
        }
    }

    fclose(fp);

    return EXOREDIS_OK;

}


void
exoredis_init_ht (void)
{
    exoredis_create_ht(&ht, EXOREDIS_HASH_TABLE_SIZE);
}

exoredis_return_codes
exoredis_set_reset_bitoffset (unsigned char *key,
                              int key_len,
                              int bit_offset,
                              int bitval,
                              unsigned char *orig_bitval)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_node = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    unsigned int offset = 0;
    *orig_bitval = 0;
    unsigned char byte_val, bit_offset_le, mask = 0;
    
    ht_index = exoredis_hash_index(&ht, key, key_len);
    he_temp = ht->table[ht_index];

    he_prev = NULL;

    /* Byte offset = bit offset / 8 */
    offset = bit_offset >> 3;

    /* First check for the key/value pair */
    while(he_temp && memcmp(he_temp->key,key, key_len)) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        if ((he_temp->type != ENCODING_VALUE_TYPE_STRING) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_EX) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_PX) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_EX_PX)) {
            return EXOREDIS_WRONGTYPE;
        }
        /* Node exists. Update the value */
        if (offset < he_temp->value_len) {
            he_node = he_temp;
        } else {
            he_node = (exoredis_hash_entry *) malloc(
                       sizeof(exoredis_hash_entry) + offset + 1);
            memset(he_node, 0, sizeof(exoredis_hash_entry) + offset + 1);
            memcpy(he_node->value, he_temp->value, offset + 1);
            memcpy(he_node->key, he_temp->key, he_temp->key_len);
            he_node->next = he_temp->next;
            he_node->key_len = he_temp->key_len;
            he_node->value_len = he_temp->value_len;
            he_node->type = ENCODING_VALUE_TYPE_STRING;

            if (he_prev) {
                he_node->next = he_prev->next;
                he_prev->next = he_node;
            } else {
                ht->table[ht_index] = he_node;
            }
            free(he_temp);
        }
    } else {
        /* New node in hash */
        he_node = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry)
                                                + offset + 1);
        memset(he_node, 0, sizeof(exoredis_hash_entry) + offset + 1);
        he_node->next = NULL;
        memcpy(he_node->key, key, key_len);
        he_node->key_len = key_len;
        he_node->value_len = offset + 1;
        he_node->type = ENCODING_VALUE_TYPE_STRING;

        if (he_prev) {
            he_node->next = he_prev->next;
            he_prev->next = he_node;
        } else {
            ht->table[ht_index] = he_node;
        }
    }
    /* At this point we have he_node */
    if (!he_node) {
        return EXOREDIS_ERR;
    }

    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    byte_val = he_node->value[offset];

    *orig_bitval = (byte_val >> bit_offset_le) & 0x1;

    if (*orig_bitval != ( 0x1 & bitval)) {
        /* Flip the bit */
        mask = (*orig_bitval)? (~(*orig_bitval << bit_offset_le)): 
               ((~(*orig_bitval) & 0x1) << bit_offset_le);
        he_node->value[offset] = (*orig_bitval)? (mask & byte_val) : (mask | byte_val);
    }
    return EXOREDIS_OK;

}

exoredis_return_codes
exoredis_get_bitoffset (unsigned char *key,
                        int key_len,
                        int bit_offset,
                        unsigned char *orig_bitval)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_node = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    unsigned int offset = 0;
    *orig_bitval = 0;
    char byte_val, bit_offset_le;
    
    ht_index = exoredis_hash_index(&ht, key, key_len);
    he_temp = ht->table[ht_index];

    he_prev = NULL;

    /* Byte offset = bit offset / 8 */
    offset = bit_offset >> 3;

    /* First check for the key/value pair */
    while(he_temp && memcmp(he_temp->key,key, key_len)) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        if ((he_temp->type != ENCODING_VALUE_TYPE_STRING) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_EX) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_PX) &&
            (he_temp->type != ENCODING_VALUE_TYPE_STRING_SEC_EX_PX)) {
            return EXOREDIS_WRONGTYPE;
        }
        /* Node exists. Update the value */
        if (offset < he_temp->value_len) {
            he_node = he_temp;
        } else {
            he_node = (exoredis_hash_entry *) malloc(
                       sizeof(exoredis_hash_entry) + offset + 1);
            memset(he_node, 0, sizeof(exoredis_hash_entry) + offset + 1);
            memcpy(he_node->value, he_temp->value, offset + 1);
            memcpy(he_node->key, he_temp->key, he_temp->key_len);
            he_node->next = he_temp->next;
            he_node->key_len = he_temp->key_len;
            he_node->value_len = he_temp->value_len;
            he_node->type = ENCODING_VALUE_TYPE_STRING;

            if (he_prev) {
                he_node->next = he_prev->next;
                he_prev->next = he_node;
            } else {
                ht->table[ht_index] = he_node;
            }
            free(he_temp);

        }
    } else {
        he_node = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry)
                                                + offset + 1);
        memset(he_node, 0, sizeof(exoredis_hash_entry) + offset + 1);
        he_node->next = NULL;
        memcpy(he_node->key, key, key_len);
        he_node->key_len = key_len;
        he_node->value_len = offset + 1;
        he_node->type = ENCODING_VALUE_TYPE_STRING;

        if (he_prev) {
            he_node->next = he_prev->next;
            he_prev->next = he_node;
        } else {
            ht->table[ht_index] = he_node;
        }
    }
    /* At this point we have he_node */
    if (!he_node) {
        return EXOREDIS_ERR;
    }
    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    byte_val = he_node->value[offset];

    *orig_bitval = (byte_val >> bit_offset_le) & 0x1;

    return EXOREDIS_OK;

}

exoredis_hash_entry *
exoredis_purge_ss_entries (exoredis_hash_entry *he_node,
                           unsigned char *key,
                           int key_len,
                           ss_entry *firstnode)
{
    ss_entry *temp = NULL, *node = NULL, *prev = NULL;
    unsigned char *ss_value = NULL;
    int ss_value_len = 0;
    exoredis_hash_entry *he_ret = NULL;


    if ((!he_node) || he_node->value_len == 0) {
        if (!firstnode) {
            return NULL;
        }
        ss_value = (unsigned char *)firstnode;
        ss_value_len = sizeof(ss_entry) + sizeof(ss_val_list) + 
                       firstnode->start_value->ss_value_len;
        he_ret = exoredis_lookup_create_update_he(key, key_len, ss_value,
                                                  ss_value_len, 0, NULL,
                                                  ENCODING_VALUE_TYPE_SORTED_SET);

        if (firstnode->start_value) {
            free(firstnode->start_value);
        }

        free(firstnode);
        return he_ret;
    }

    temp = (ss_entry *)he_node->value;

    if ((he_node->value_len > 0) && temp->card == 0) {
        if (temp->next) {
            he_ret = exoredis_lookup_create_update_he(key, key_len, 
                                      (unsigned char *)temp->next,
                                      sizeof(ss_entry) + sizeof(ss_val_list) +
                                      temp->next->start_value->ss_value_len, 0,
                                      NULL, ENCODING_VALUE_TYPE_SORTED_SET);
    } else {
            return exoredis_lookup_create_update_he(key, key_len, NULL, 0, 0,
                                            NULL,
                                            ENCODING_VALUE_TYPE_SORTED_SET);
        }
    }

    /* Break at first non-zero */
    temp = temp->next;
    while(temp && temp->card == 0) {
        node = temp;
        temp = temp->next;
        free(node);
    }

    if (!temp) {
        if (firstnode) {
            ss_value = (unsigned char *)firstnode;
            ss_value_len = sizeof(ss_entry) + sizeof(ss_val_list) + 
                           firstnode->start_value->ss_value_len;
            he_ret =  exoredis_lookup_create_update_he(key, key_len, ss_value,
                                                       ss_value_len, 0, NULL,
                                                       ENCODING_VALUE_TYPE_SORTED_SET);
            if (firstnode->start_value) {
                free(firstnode->start_value);
            }

            free(firstnode);
            return he_ret;

        } else {
            return exoredis_lookup_create_update_he(key, key_len, NULL, 0, 0, NULL,
                                                    ENCODING_VALUE_TYPE_SORTED_SET);
        }
    }

    ss_value = (unsigned char *)temp;
    ss_value_len = sizeof(ss_entry) + sizeof(ss_val_list) + 
                   temp->start_value->ss_value_len;


    /* Purge remaining scores as well */
    prev = temp;
    temp = temp->next;
    while(temp) {
        if (temp->card == 0) {
            node = temp;
            temp = temp->next;
            prev->next = temp;
            free(node);
        }
        prev = temp;
        temp = temp->next;
    }

    return exoredis_lookup_create_update_he(key, key_len, ss_value,
                                            ss_value_len, 0, NULL,
                                            ENCODING_VALUE_TYPE_SORTED_SET);

}

exoredis_return_codes
exoredis_form_ss_value_and_insert (unsigned char *key,
                                   int key_len,
                                   int *score,
                                   unsigned char **value,
                                   int *value_len,
                                   int num,
                                   unsigned int *added)
{
    int i = 0;
    exoredis_hash_entry *he_node = NULL;
    ss_entry *temp_ss_entry = NULL, *new_ss_entry = NULL;
    ss_entry *prev_ss_entry = NULL, *temp2_ss_entry = NULL;
    ss_val_list *exist_val_list = NULL, *temp_val_list = NULL; 
    ss_val_list *new_val_list = NULL, *prev_val_list = NULL;
    char scorematch = 0 , cmp = 0;

    *added = 0;

    /* Lookup if this an existing key */
    he_node = exoredis_read_he(key, key_len, NULL, NULL, NULL);

    if (!he_node) {
        /* First create a blank placeholder */
        he_node = exoredis_lookup_create_update_he(key, key_len, NULL, 0, 0,
                                                NULL,
                                                ENCODING_VALUE_TYPE_SORTED_SET);
    }

    if (he_node) {
        if (he_node->type != ENCODING_VALUE_TYPE_SORTED_SET) {
            return EXOREDIS_WRONGTYPE;
        }
        for (i = 0; i < num; i++) {
            /* Each (value, value...) is an ss_val_list */
            /* Each (score, card, value...) is an ss_entry */
            /* Check if score exists */
            /* Do the following logic only if something realy exists in val */
            scorematch = 0;
            new_ss_entry = NULL;
            temp_ss_entry = NULL;
            prev_ss_entry = NULL;
            exist_val_list = NULL;
            if (he_node->value_len > 0) {
                temp_ss_entry = (ss_entry *)he_node->value;
                while(temp_ss_entry && (temp_ss_entry->score <= score[i])) {
                    if (score[i] == temp_ss_entry->score) {
                        /* Score match found */
                        scorematch = 1;
                        break;
                    }
                    
                    /* While around, check if we have to re-house the 
                     * value from this score
                     */
                    if (!exist_val_list) {
                        temp_val_list = temp_ss_entry->start_value;
                        new_val_list = NULL;
                        prev_val_list = NULL;
                        while(temp_val_list && ((cmp = memcmp(temp_val_list->ss_value,
                                                value[i], value_len[i])) <= 0)) {
                            if (cmp == 0) {
                                /* Value match found */
                                break;
                            }
                            prev_val_list = temp_val_list;
                            temp_val_list = temp_val_list->ss_next;
                        }

                        if (temp_val_list && (cmp == 0)) {
                            temp_ss_entry->card--;
                            exist_val_list = temp_val_list;

                            if(prev_val_list) {
                                /* Middle of the list */
                                prev_val_list->ss_next = temp_val_list->ss_next;
                            } else {
                                /* First in list */
                                temp_ss_entry->start_value = temp_val_list->ss_next;
                            }
                        }
                    }
                    prev_ss_entry = temp_ss_entry;
                    temp_ss_entry = temp_ss_entry->next;
                }

                /* Check for left over scores having this value */
                if (!exist_val_list && temp_ss_entry) {
                    temp2_ss_entry = temp_ss_entry->next;
                    while(temp2_ss_entry) {
                        temp_val_list = temp2_ss_entry->start_value;
                        new_val_list = NULL;
                        prev_val_list = NULL;
                        while(temp_val_list && ((cmp = memcmp(temp_val_list->ss_value,
                                                value[i], value_len[i])) <= 0)) {
                            if (cmp == 0) {
                                /* Value match found */
                                break;
                            }
                            prev_val_list = temp_val_list;
                            temp_val_list = temp_val_list->ss_next;
                        }

                        if (temp_val_list && (cmp == 0)) {
                            /* Delete value at this score */
                            if(prev_val_list) {
                                /* Middle of the list */
                                prev_val_list->ss_next = temp_val_list->ss_next;
                            } else {
                                /* First in list */
                                temp_ss_entry->start_value = temp_val_list->ss_next;
                            }
                            temp_ss_entry->card--;
                            exist_val_list = temp_val_list;
                            break;
                        }
                        temp2_ss_entry = temp2_ss_entry->next;
                    }
                }
            }

            /* At this point we have either a) a score match, or b) a new score
             * block of ss_entry, or c) an update requiring value block of 
             * ss_val_list. For the second case, either the position is given 
             * just before temp (if temp is NOT NULL), or at the start/end of
             * he_node->value
             */
            
            if (scorematch) {
                /* Check if value already exists in ss_val_list */
                temp_val_list = temp_ss_entry->start_value;
                new_val_list = NULL;
                prev_val_list = NULL;
                while(temp_val_list && ((cmp = memcmp(temp_val_list->ss_value,
                                        value[i], value_len[i])) <= 0)) {
                    if (cmp == 0) {
                        /* Value match found */
                        break;
                    }
                    prev_val_list = temp_val_list;
                    temp_val_list = temp_val_list->ss_next;
                }

                if (cmp == 0) {
                    /* Nothing to do for this iteration of i */
                    continue;
                }

                if (!exist_val_list) {
                    /* Value match not found. Create a new ss_val_list and insert*/
                    new_val_list = (ss_val_list *) malloc (sizeof(ss_val_list) 
                                                           + value_len[i]);
                    new_val_list->ss_value_len = value_len[i];
                    new_val_list->ss_next = NULL;
                    memcpy(new_val_list->ss_value, value[i], value_len[i]);

                    /* Value added */
                    (*added)++;
                } else {
                    new_val_list = exist_val_list;
                }

                /* Increase the cardinality */
                temp_ss_entry->card++;

                /* Insert new_val_list into the correct position in list */
                if (temp_val_list) {
                    if(prev_val_list) {
                        /* Middle of the list */
                        prev_val_list->ss_next = new_val_list;
                        new_val_list->ss_next = temp_val_list;
                    } else {
                        /* First in list */
                        temp_ss_entry->start_value = new_val_list;
                        new_val_list->ss_next = temp_val_list;
                    }
                } else {
                    if (prev_val_list) {
                        /* Last in list */
                        prev_val_list->ss_next = new_val_list;
                    } else {
                        /* Non-existant list */
                        temp_ss_entry->start_value = new_val_list;
                    }
                }
            } else {
                /* No score match in all of exist_val; 
                 * insert a new ss_entry 
                 */
                new_ss_entry = (ss_entry *)malloc(sizeof(ss_entry));
                new_ss_entry->card = 1;
                new_ss_entry->next = NULL;
                new_ss_entry->score = score[i];

                if (!exist_val_list) {
                    new_ss_entry->start_value = (ss_val_list *)malloc(
                                                 sizeof(ss_val_list) + 
                                                 value_len[i]);
                    memcpy(new_ss_entry->start_value->ss_value, value[i], 
                           value_len[i]);
                    new_ss_entry->start_value->ss_value_len = value_len[i];
                    new_ss_entry->start_value->ss_next = NULL;

                    /* Value added */
                    (*added)++;
                } else {
                    new_ss_entry->start_value = exist_val_list;
                    new_ss_entry->start_value->ss_next = NULL;

                }

                /* No score matches, check for position in exist_val */
                if (temp_ss_entry) {
                    if (prev_ss_entry) {
                        /* Middle of the entries */
                        prev_ss_entry->next = new_ss_entry;
                        new_ss_entry->next = temp_ss_entry;
                    } else {
                        /* First in the list */ 
                        new_ss_entry->next = temp_ss_entry;
                    }
                } else {
                    if (prev_ss_entry) {
                        /* Last entry */
                        prev_ss_entry->next = new_ss_entry;
                    }
                }
            }

            he_node = exoredis_purge_ss_entries(he_node, key, key_len,
                                                new_ss_entry);
        } /* close i loop */
    } else {
        /* Shouldn't get here */
        return EXOREDIS_ERR;
    }
    return EXOREDIS_OK;
}

exoredis_return_codes
exoredis_add_sortedset (unsigned char *key,
                        int key_len,
                        int *score,
                        unsigned char **value,
                        int *value_len,
                        int num,
                        unsigned int *added)
{
    int i = 0;
    char temp_str[100];
    int len = 0;
    exoredis_return_codes ret;

    printf("ZADD %s (%d) ", key, key_len);
    while (i < num) {
        memcpy(temp_str, value[i], value_len[i]);
        len = value_len[i];
        temp_str[len] = '\0';
        printf("%d %s ", score[i], temp_str);
        i++;
    }

    /* Form sorted set value */
    ret = exoredis_form_ss_value_and_insert(key, key_len, score, value, 
                                            value_len, num, added);
    return ret;
}


void
exoredis_make_bulk_string_from_sorted_set(ss_entry *start,
                                          int start_index,
                                          int delta,
                                          unsigned char withscore,
                                          unsigned char **buf,
                                          int *buf_len,
                                          int *size,
                                          int save_format)
{
    ss_entry *temp = start;
    ss_val_list *temp_val_list = NULL;
    int index = 0, len=0;
    *buf_len = 0;
    unsigned char scorebuf[12];

    temp_val_list = temp->start_value;
    while(index < start_index) {
        temp_val_list = temp_val_list->ss_next;
        index++;
    }
    index = 0;
    while (index < delta) {
        while(temp_val_list) {
            if (!save_format) {
                exoredis_resp_bulk_string(temp_val_list->ss_value, 
                                          temp_val_list->ss_value_len, 0, buf,
                                          buf_len);
                if(withscore) {
                    len = sprintf((char *)scorebuf, "%d", temp->score);
                    exoredis_resp_bulk_string(scorebuf, len, 0, buf, buf_len);
                }
            } else {
                memcpy(*buf, &temp_val_list->ss_value_len,
                       sizeof(temp_val_list->ss_value_len));
                (*buf) += sizeof(temp_val_list->ss_value_len);
                (*buf_len) += sizeof(temp_val_list->ss_value_len);
                memcpy(*buf, temp_val_list->ss_value, 
                       temp_val_list->ss_value_len);
                (*buf) += temp_val_list->ss_value_len;
                (*buf_len) += temp_val_list->ss_value_len;
                if(withscore) {
                    memcpy(*buf, &temp->score, sizeof(temp->score)); 
                    (*buf) += sizeof(temp->score);
                    (*buf_len) += sizeof(temp->score); 
                }
            }
            temp_val_list = temp_val_list->ss_next;
            index++;
            if (index == delta) break;
        }
        temp = temp->next;
        temp_val_list = temp->start_value;
    }
    *size = delta;
}

exoredis_return_codes
exoredis_find_range (unsigned char *key,
                     int key_len,
                     int min,
                     int max,
                     unsigned char withscore,
                     unsigned char **buf,
                     int *buf_len,
                     int *size,
                     int save_format)
{
    ss_entry *start = NULL;
    int index = min;
    exoredis_hash_entry *he_node = NULL;
    *size = 0;
    *buf_len = 0;

    /* Lookup if this an existing key */
    he_node = exoredis_read_he(key, key_len, NULL, NULL, NULL);

    if ((!he_node) || (he_node->value_len == 0) || (min > max)) {
        /* Return a 0 array */
        return EXOREDIS_ERR;
    }

    if (he_node->type != ENCODING_VALUE_TYPE_SORTED_SET) {
        return EXOREDIS_WRONGTYPE;
    }
    start = (ss_entry *)he_node->value;
    while(start && start->card < index) {
        index -= start->card;
        start = start->next;
    }

    exoredis_make_bulk_string_from_sorted_set(start, index, max - min + 1, 
                                              withscore, buf, buf_len, size,
                                              save_format);

    return EXOREDIS_OK;

}


exoredis_return_codes
exoredis_range_sortedset (unsigned char *key,
                          int key_len,
                          int min,
                          int max,
                          unsigned char withscore,
                          unsigned char **buf,
                          int *buf_len,
                          int *size,
                          char save_format)
{
    exoredis_return_codes ret;
    printf("ZRANGE %s (%d) %d %d %s\n", key, key_len, min, max, 
           withscore?"withscores":"");

    ret = exoredis_find_range(key, key_len, min, max, withscore,
                              buf, buf_len, size, save_format);

    return ret;
}


exoredis_return_codes
exoredis_find_count (unsigned char *key,
                     int key_len,
                     int min,
                     int max,
                     int *count)
{
    ss_entry *start = NULL;
    exoredis_hash_entry *he_node = NULL;
    *count = 0;

    /* Lookup if this an existing key */
    he_node = exoredis_read_he(key, key_len, NULL, NULL, NULL);

    if ((!he_node) || (he_node->value_len == 0) || (min > max)) {
        /* Return a 0 count */
        return EXOREDIS_ERR;
    }
    if (he_node->type != ENCODING_VALUE_TYPE_SORTED_SET) {
        return EXOREDIS_WRONGTYPE;
    }

    start = (ss_entry *)he_node->value;
    while(start && start->score < min) {
        start = start->next;
    }

    while(start && start->score <= max) {
        *count += start->card;
        start = start->next;
    }
    return EXOREDIS_OK;
}

exoredis_return_codes
exoredis_count_sortedset (unsigned char *key,
                          int key_len,
                          int min,
                          int max,
                          int *count)
{
    printf("ZCOUNT %s (%d) %d %d\n", key, key_len, min, max);

    return exoredis_find_count(key, key_len, min, max, count);

}

exoredis_return_codes
exoredis_card_sortedset (unsigned char *key,
                         int key_len,
                         int *card)
{
    ss_entry *start = NULL;
    exoredis_hash_entry *he_node = NULL;
    *card = 0;

    printf("ZCARD %s (%d)\n", key, key_len);
    /* Lookup if this an existing key */
    he_node = exoredis_read_he(key, key_len, NULL, NULL, NULL);

    if ((!he_node) || (he_node->value_len == 0) || 
        (he_node->type != ENCODING_VALUE_TYPE_SORTED_SET)) {
        /* Return a 0 count */
        return EXOREDIS_ERR;
    }
    if (he_node->type != ENCODING_VALUE_TYPE_SORTED_SET) {
        return EXOREDIS_WRONGTYPE;
    }

    start = (ss_entry *)he_node->value;
    while(start) {
        *card += start->card;
        start = start->next;
    }
    return EXOREDIS_OK;
    
}


#ifdef HASH_TEST_MODE

/*********************************************/
/*********************TESTING ****************/

main()
{
    exoredis_create_ht(&ht, EXOREDIS_HASH_TABLE_SIZE);
    exoredis_lookup_create_update_he("somekey", strlen("somekey"), "somekeyval", strlen("somekeyval"));
    exoredis_lookup_create_update_he("treetop animal", strlen("treetop animal"), "monkey", strlen("monkey"));
    exoredis_lookup_create_update_he("my\tbinary\t\nkey", 17, "and\tnow\tmy\binary\val\r\n", 27);
    exoredis_dump_ht(&ht);
    exoredis_read_he("somekey",strlen("somekey"));
    exoredis_read_he("my\tbinary\t\nkey",17);
    exoredis_dump_ht(&ht);
    exoredis_destroy_he("somekey", strlen("somekey"));
    exoredis_destroy_he("treetop animal", strlen("treetop animal"));
    exoredis_dump_ht(&ht);
    exoredis_destroy_ht(&ht);
}

#endif
