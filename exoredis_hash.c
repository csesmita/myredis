/*
 * exoredis_hash.c:
 * Implements the hash utilities
 */

#include "exoredis.h"
#include "exoredis_server.h"
#include "exordb.h"
#include "exoredis_hash.h"

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

    while (i < ht->size) {
        temp = ht->table[i];
        while (temp) {
            printf("Got key value pair (%u,%u)\n", temp->key, temp->value);
            temp = temp->next;
        }
        i++;
    }
}

unsigned int
exoredis_hash_index (unsigned int key)
{
    unsigned long hash_value = 0;
    unsigned int seed = EXOREDIS_HASH_SEED;
    int i = 0;

    for (i = 0; i < sizeof(key) && key != 0; i++) {
        hash_value += key & seed;
        key = key << 1;
    }
    printf("Hash value for key %d is %d\n", key, (unsigned int)(hash_value % ht->size));
    return (unsigned int)(hash_value % ht->size);
}

void
exoredis_destroy_he (unsigned int key)
{
    exoredis_hash_entry *he_start = NULL;
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    /* First check for the key/value pair */
    int ht_index = 0;
    
    ht_index = exoredis_hash_index(key);
    he_start = ht->table[ht_index];

    he_temp = he_start;
    he_prev = NULL;

    while(he_temp && he_temp->key != key) {
        he_prev = he_temp;
        he_temp = he_temp->next;
    }

    if (he_temp) {
        /* Node exists. Destroy it */
        if (he_temp == he_start) {
            he_start = he_start->next;
            free(he_start);
            he_start = NULL;
        } else {
            he_prev->next = he_temp->next;
            free(he_temp);
            he_temp = NULL;
        }
    } else {
        /* Node doesn't exist. Create a new entry */
        he_temp = (exoredis_hash_entry *)malloc(sizeof(exoredis_hash_entry));
        if(he_prev) {
            he_temp->next = he_prev->next;
            he_prev->next = he_temp;
        } else {
            he_start = he_temp;
        }
    }
}

void
exoredis_create_update_he (unsigned int key, 
                           unsigned int value)
{
    exoredis_hash_entry *he_start = NULL;
    exoredis_hash_entry *he_temp = NULL;
    exoredis_hash_entry *he_prev = NULL;
    /* First check for the key/value pair */
    int ht_index = 0;
    
    ht_index = exoredis_hash_index(key);
    he_start = ht->table[ht_index];

    he_temp = he_start;
    he_prev = NULL;

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
            he_start = he_temp;
        }
    }

}

#if HASH_TEST_MODE

/*********************************************/
/*********************TESTING ****************/

main()
{
    exoredis_create_ht(40);
    exoredis_dump_ht();
    exoredis_create_update_he(198,17);
    exoredis_dump_ht();
#if 0
    exoredis_create_update_he(223, 34);
    exoredis_create_update_he(198, 35);
    exoredis_create_update_he(223, 34);
    exoredis_create_update_he(225, 34);
    exoredis_create_update_he(96, 34);
    exoredis_create_update_he(3696, 34);
    exoredis_destroy_he(198);
    exoredis_destroy_he(134);
    exoredis_dump_ht();
    exoredis_destroy_ht();
    exoredis_dump_ht();
#endif
}

#endif
