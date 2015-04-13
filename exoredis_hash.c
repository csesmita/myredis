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

void
exoredis_lookup_create_update_he (unsigned char *key, 
                                  int key_len,
                                  unsigned char *value,
                                  int value_len)
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

            if (he_prev) {
                he_temp->next = he_prev->next;
                he_prev->next = he_temp;
            } else {
                new_ht->table[ht_index] = he_temp;
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
}

void *
exoredis_read_he (unsigned char *key,
                  int key_len,
                  unsigned char **value,
                  int *value_len)
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
        *value = (unsigned char *)malloc(he_temp->value_len);
        memcpy(*value, he_temp->value, he_temp->value_len);
        *value_len = he_temp->value_len;
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
        *value = (unsigned char *)malloc(he_temp->value_len);
        memcpy(*value, he_temp->value, he_temp->value_len);
        *value_len = he_temp->value_len;
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

    while (i < ht->size) {
        temp = ht->table[i];
        while(temp) {
            /* Write (key,value) to file starting from the start*/
            rewind(exoredis_io.dbfp);


            temp = temp->next;
        }
        i++;
    }
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
                              unsigned int bit_offset,
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
    byte_val = he_node->value[offset];

    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    *orig_bitval = (byte_val >> bit_offset_le) & 1;
    
    printf("Original bit %d new bit %d\n", *orig_bitval, bitval);
    printf("Original byte %d ",he_node->value[offset]);
    if (*orig_bitval != ( 0x1 & bitval)) {
        /* Flip the bit */
        mask = ~(~(*orig_bitval) << bit_offset_le);
        he_node->value[offset] = (*orig_bitval)? (mask & byte_val) : (mask | byte_val);
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
                        unsigned int bit_offset,
                        unsigned char *orig_bitval)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_node = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    unsigned int offset = 0;
    *orig_bitval = 0;
    char byte_val, bit_offset_le;
    unsigned int val_int;
    char *ptr;
    
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
    val_int = atoi((char *)(he_node->value));
    ptr = (char *)(&val_int);
    byte_val = ptr[offset];

    bit_offset_le = EXOREDIS_LITTLE_ENDIAN_BIT_POS((bit_offset - (offset << 3)));

    *orig_bitval = (byte_val >> bit_offset_le) & 1;
    printf("Dumping hash tree \n");
    exoredis_dump_ht(&ht);
    printf("Dumping new hash tree \n");
    exoredis_dump_ht(&new_ht);

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
