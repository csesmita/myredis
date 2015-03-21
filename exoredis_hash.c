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
exoredis_create_ht (unsigned size)
{
    int i = 0;
    ht = (exoredis_ht *) malloc(sizeof(exoredis_ht));
    ht-> size = size;
    ht->table = (exoredis_hash_entry **) malloc(size * sizeof(exoredis_hash_entry *));
    while (i < size) {
        ht->table[i++] = NULL;
    }
}

void
exoredis_destroy_ht ( void )
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    if (!ht) {
        return;
    }

    while (i < ht->size) {
        while(ht->table[i]) {
            temp = ht->table[i];
            ht->table[i] = ht->table[i]->next;
            free(temp);
        }
        i++; 
    }

    free(ht);
    ht = NULL;
}

void
exoredis_dump_ht ( void )
{
    int i = 0;
    exoredis_hash_entry *temp = NULL;

    if(!ht) {
        return;
    }

    while (i < ht->size) {
        temp = ht->table[i];
        while (temp) {
            printf("Hash %d:Got key value pair (%u,%u)\n", i, temp->key, temp->value);
            temp = temp->next;
        }
        i++;
    }
}

unsigned int
exoredis_hash_index (unsigned char *key)
{
    unsigned long hash_value = 0;
    unsigned int seed = EXOREDIS_HASH_SEED;
    int i = 0;
    char *temp_key = key;

    # if 0
    for (i = 0; i < sizeof(temp_key) && temp_key != 0; i++) {
        hash_value += temp_key & seed;
        temp_key = temp_key << 1;
    }
    #endif
    while (hash_value < ULONG_MAX && i < strlen(temp_key)) {
        hash_value = hash_value << 8;
        hash_value += temp_key[i++];
    }
    printf("Hash value for key %u is %u\n", (unsigned int)(key), (unsigned int)(hash_value % ht->size));
    return (unsigned int)(hash_value % ht->size);
}

void
exoredis_destroy_he (unsigned int key)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index((unsigned char *)&key);

    he_temp = ht->table[ht_index];
    he_prev = NULL;

    /* First check for the key/value pair */
    while(he_temp && he_temp->key != key) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Destroy it */
        if (he_temp == ht->table[ht_index]) {
            ht->table[ht_index] = ht->table[ht_index]->next;
        } else {
            he_prev->next = he_temp->next;
        }
        free(he_temp);
    } else {
        /* Node doesn't exist. NOP */
        printf("Key %d not found\n", key);
    }
}

void
exoredis_create_update_he (unsigned int key, 
                           unsigned int value)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index((unsigned char *)&key);
    he_temp = ht->table[ht_index];

    he_prev = NULL;

    /* First check for the key/value pair */
    while(he_temp && he_temp->key != key) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Update the value */
        he_temp->value = value;
    } else {
        /* Node doesn't exist. Create a new entry */
        he_temp = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry));
        he_temp->next = NULL;
        he_temp->key = key;
        he_temp->value = value;
        if (he_prev) {
            he_temp->next = he_prev->next;
            he_prev->next = he_temp;
        } else {
            ht->table[ht_index] = he_temp;
        }
    }
}

unsigned int
exoredis_read_he (unsigned int key)
{
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    int ht_index = 0;
    
    ht_index = exoredis_hash_index((unsigned char *)&key);
    he_temp = ht->table[ht_index];

    he_prev = NULL;

    /* First check for the key/value pair */
    while(he_temp && he_temp->key != key) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Return the value */
        printf("Returning key value pair (%d,%d)\n",he_temp->key, he_temp->value);
        return he_temp->value;
    }
    printf("Key %d not found in DB\n",key);
    return EXOREDIS_DB_VAL_INVALID;
}

#if HASH_TEST_MODE

/*********************************************/
/*********************TESTING ****************/

main()
{
    exoredis_create_ht(40);
    exoredis_create_update_he(198,17);
    exoredis_create_update_he(223, 34);
    exoredis_create_update_he(198, 35);
    exoredis_create_update_he(223, 34);
    exoredis_create_update_he(225, 34);
    exoredis_create_update_he(96, 34);
    exoredis_create_update_he(3696, 34);
    exoredis_dump_ht();
    exoredis_read_he(198);
    exoredis_destroy_he(198);
    exoredis_destroy_he(134);
    exoredis_destroy_ht();
}

#endif
