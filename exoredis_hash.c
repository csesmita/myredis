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
exoredis_ht *new_ht = NULL;

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

        ht_index = exoredis_hash_index(&new_ht, key, key_len);

        he_temp = new_ht->table[ht_index];
        he_prev = NULL;

        /* First check for the key/value pair */
        while(he_temp && memcmp(he_temp->key, key, key_len)) {
            he_prev = he_temp;
            he_temp = he_temp->next;
        }

        if (he_temp) {
            /* Node exists. Destroy it */
            if (he_temp == new_ht->table[ht_index]) {
                new_ht->table[ht_index] = new_ht->table[ht_index]->next;
            } else if (he_prev) {
                he_prev->next = he_temp->next;
            }
            free(he_temp);
        } else {
            /* Node doesn't exist. NOP */
            printf("Key %s not found in Create Hash as well\n", key);
        }

    }
}

exoredis_hash_entry *
exoredis_lookup_create_update_he (unsigned char *key, 
                                  int key_len,
                                  unsigned char *value,
                                  int value_len,
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
        /* Node exists. Update the value */
        he_temp->dirty = TRUE;
        if (value_len == he_temp->value_len) {
            memcpy(he_temp->value, value, value_len);
        } else {
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

            if (he_prev) {
                he_temp->next = he_prev->next;
                he_prev->next = he_temp;
            } else {
                new_ht->table[ht_index] = he_temp;
            }

        }
    } else {
        /* Node doesn't exist. Lookup in the new hash */
        ht_index = exoredis_hash_index(&new_ht, key, key_len);
        he_temp = new_ht->table[ht_index];

        he_prev = NULL;

        /* First check for the key/value pair */
        while(he_temp && memcmp(he_temp->key, key, key_len)) {
            he_prev = he_temp;
            he_temp = he_temp->next;
        }

        if (he_temp) {
            if (value_len == he_temp->value_len) {
                memcpy(he_temp->value, value, value_len);
            } else {
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

                if (he_prev) {
                    he_temp->next = he_prev->next;
                    he_prev->next = he_temp;
                } else {
                    new_ht->table[ht_index] = he_temp;
                }
            }
        } else {
            he_temp = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry)
                                                    + value_len);
            memset(he_temp, 0, sizeof(exoredis_hash_entry) + value_len);
            he_temp->next = NULL;
            memcpy(he_temp->key, key, key_len);
            memcpy(he_temp->value, value, value_len);
            he_temp->key_len = key_len;
            he_temp->value_len = value_len;
            he_temp->type = type;

            if (he_prev) {
                he_temp->next = he_prev->next;
                he_prev->next = he_temp;
            } else {
                new_ht->table[ht_index] = he_temp;
            }
        }
    }
    printf("Dumping hash tree \n");
    exoredis_dump_ht(&ht);
    printf("Dumping new hash tree \n");
    exoredis_dump_ht(&new_ht);

    return he_temp;
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

    /* Now check the New hash */
    ht_index = exoredis_hash_index(&new_ht, key, key_len);
    he_temp = new_ht->table[ht_index];

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

/* Dump the entries in the HT to the DB file */
void
exoredis_feed_ht_to_io (void)
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    /* Write (key,value) to file starting from the start */
    rewind(exoredis_io.dbfp);


}

/* Copy the entry from new hash to update hash and delete the entry */
void
exoredis_copy_to_update_hash_and_delete (unsigned char *key,
                                         int key_len,
                                         exoredis_hash_entry **he_input,
                                         exoredis_hash_entry **he_input_prev,
                                         int new_t_index)
{
    exoredis_hash_entry *he_update = NULL;
    int ht_index = 0;
    int value_len = 0;

    if (!he_input || !*he_input) {
        return;
    }

    /* Add new entry into ht */
    ht_index = exoredis_hash_index(&ht, key, key_len);
    he_update  = ht->table[ht_index];

    /* Insert into front of hash index, to save walk to the end */
    value_len = (*he_input)->value_len;
    he_update = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry) 
                                              + value_len);
    memset(he_update, 0, sizeof(exoredis_hash_entry) + value_len);
    memcpy(he_update->key,(*he_input)->key, (*he_input)->key_len);
    memcpy(he_update->value,(*he_input)->value, (*he_input)->value_len);
    he_update->next = ht->table[ht_index];
    ht->table[ht_index] = he_update;

    /* Now delete the node from new hash */
    if ((*he_input) == new_ht->table[new_t_index]) {
        /* First entry at the index */
        new_ht->table[new_t_index] = new_ht->table[new_t_index]->next;
    } else if (he_input_prev && *he_input_prev) {
        /* Node exists midway or at the end */
        (*he_input_prev)->next = (*he_input)->next;
    }
    free(*he_input);
    *he_input = NULL;
}

void
exoredis_init_ht (void)
{
    exoredis_create_ht(&ht, EXOREDIS_HASH_TABLE_SIZE);
    exoredis_create_ht(&new_ht, EXOREDIS_HASH_TABLE_SIZE);
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
        if (he_temp->type != ENCODING_VALUE_TYPE_STRING) {
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
                new_ht->table[ht_index] = he_node;
            }
            free(he_temp);
        }
        he_node->dirty = TRUE;
    } else {
        /* Node doesn't exist. Lookup in the new hash */
        ht_index = exoredis_hash_index(&new_ht, key, key_len);
        he_temp = new_ht->table[ht_index];

        he_prev = NULL;

        /* First check for the key/value pair */
        while(he_temp && memcmp(he_temp->key, key, key_len)) {
            he_prev = he_temp;
            he_temp = he_temp->next;
        }

        if (he_temp) {
            if (he_temp->type != ENCODING_VALUE_TYPE_STRING) {
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
                    new_ht->table[ht_index] = he_node;
                }
                free(he_temp);

            }
        } else {
            /* New node in new hash */
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
                new_ht->table[ht_index] = he_node;
            }
        }
    }
    /* At this point we have he_node */
    if (!he_node) {
        return EXOREDIS_ERR;
    }

    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    byte_val = he_node->value[offset];

    *orig_bitval = (byte_val >> bit_offset_le) & 0x1;

    printf("Original bit %d new bit %d\n", *orig_bitval, bitval);
    printf("Original byte %d\n",he_node->value[offset]);
    if (*orig_bitval != ( 0x1 & bitval)) {
        /* Flip the bit */
        mask = (*orig_bitval)? (~(*orig_bitval << bit_offset_le)): 
               ((~(*orig_bitval) & 0x1) << bit_offset_le);
        he_node->value[offset] = (*orig_bitval)? (mask & byte_val) : (mask | byte_val);
        printf("Modified byte value %d\n", he_node->value[offset]);
    }
    printf("Dumping hash tree \n");
    exoredis_dump_ht(&ht);
    printf("Dumping new hash tree \n");
    exoredis_dump_ht(&new_ht);

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
        if (he_temp->type != ENCODING_VALUE_TYPE_STRING) {
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
                new_ht->table[ht_index] = he_node;
            }
            free(he_temp);

        }
    } else {
        /* Node doesn't exist. Lookup in the new hash */
        ht_index = exoredis_hash_index(&new_ht, key, key_len);
        he_temp = new_ht->table[ht_index];

        he_prev = NULL;

        /* First check for the key/value pair */
        while(he_temp && memcmp(he_temp->key, key, key_len)) {
            he_prev = he_temp;
            he_temp = he_temp->next;
        }

        if (he_temp) {
            if (he_temp->type != ENCODING_VALUE_TYPE_STRING) {
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
                    new_ht->table[ht_index] = he_node;
                }
                free(he_temp);

            }
        } else {
            /* New node in new hash */
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
                new_ht->table[ht_index] = he_node;
            }
        }
    }
    /* At this point we have he_node */
    if (!he_node) {
        return EXOREDIS_ERR;
    }
    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    byte_val = he_node->value[offset];

    *orig_bitval = (byte_val >> bit_offset_le) & 0x1;
    printf("Dumping hash tree \n");
    exoredis_dump_ht(&ht);
    printf("Dumping new hash tree \n");
    exoredis_dump_ht(&new_ht);

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
                                                  ss_value_len,
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
                                                      temp->next->start_value->ss_value_len,
                                                      ENCODING_VALUE_TYPE_SORTED_SET);
        } else {
            return exoredis_lookup_create_update_he(key, key_len, NULL, 0,
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
                                                       ss_value_len,
                                                       ENCODING_VALUE_TYPE_SORTED_SET);
            if (firstnode->start_value) {
                free(firstnode->start_value);
            }

            free(firstnode);
            return he_ret;

        } else {
            return exoredis_lookup_create_update_he(key, key_len, NULL, 0,
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
                                            ss_value_len,
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
        he_node = exoredis_lookup_create_update_he(key, key_len, NULL, 0,
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
                                          int *size)
{
    ss_entry *temp = start;
    ss_val_list *temp_val_list = NULL;
    int index = 0;
    *buf_len = 0;

    temp_val_list = temp->start_value;
    while(index < start_index) {
        temp_val_list = temp_val_list->ss_next;
        index++;
    }
    index = 0;
    while (index < delta) {
        while(temp_val_list) {
            exoredis_resp_bulk_string(temp_val_list->ss_value, 
                                      temp_val_list->ss_value_len, 0, buf,
                                      buf_len);
            *buf += *buf_len;
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
                     int *size)
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
                                              withscore, buf, buf_len, size);

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
                          int *size)
{
    exoredis_return_codes ret;
    printf("ZRANGE %s (%d) %d %d %s\n", key, key_len, min, max, 
           withscore?"withscores":"");

    ret = exoredis_find_range(key, key_len, min, max, withscore,
                              buf, buf_len, size);

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
    exoredis_create_ht(&new_ht, EXOREDIS_HASH_TABLE_SIZE);
    exoredis_lookup_create_update_he("somekey", strlen("somekey"), "somekeyval", strlen("somekeyval"));
    exoredis_lookup_create_update_he("treetop animal", strlen("treetop animal"), "monkey", strlen("monkey"));
    exoredis_lookup_create_update_he("my\tbinary\t\nkey", 17, "and\tnow\tmy\binary\val\r\n", 27);
    exoredis_dump_ht(&ht);
    exoredis_dump_ht(&new_ht);
    exoredis_read_he("somekey",strlen("somekey"));
    exoredis_read_he("my\tbinary\t\nkey",17);
    exoredis_dump_ht(&ht);
    exoredis_dump_ht(&new_ht);
    exoredis_destroy_he("somekey", strlen("somekey"));
    exoredis_destroy_he("treetop animal", strlen("treetop animal"));
    exoredis_dump_ht(&ht);
    exoredis_dump_ht(&new_ht);
    exoredis_destroy_ht(&ht);
    exoredis_destroy_ht(&new_ht);
}

#endif
