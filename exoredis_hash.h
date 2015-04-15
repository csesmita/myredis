#ifndef EXOREDIS_HASH_H_
#define EXOREDIS_HASH_H_
#include "exordb.h"


/* To make the hashes work fast, adopt static length for buffers */
#define EXOREDIS_HASH_KEY_LEN 100

struct _exoredis_hash_entry {
    struct _exoredis_hash_entry *next;

    exoredis_value_type type;

    /* To make it fast, have static key strings */
    unsigned char key[EXOREDIS_HASH_KEY_LEN];
    int  key_len;
    int  value_len;
    unsigned char value[0];
};

typedef struct _exoredis_hash_entry exoredis_hash_entry;

struct _exoredis_ht {
    unsigned size;
    exoredis_hash_entry **table;
};

typedef struct _exoredis_ht exoredis_ht;

struct _sorted_set_list {
    struct _sorted_set_list *ss_next;
    unsigned int ss_value_len;
    unsigned char ss_value[0];
};

typedef struct _sorted_set_list ss_val_list;

struct _sorted_list_entry {
    int score;
    unsigned int card;
    ss_val_list *start_value;
    struct _sorted_list_entry *next;
};

typedef struct _sorted_list_entry ss_entry;

#define EXOREDIS_HASH_SEED 257
#define EXOREDIS_DB_VAL_INVALID (ULONG_MAX + 1)
#define EXOREDIS_HASH_TABLE_SIZE 200
//#define HASH_TEST_MODE

void
exoredis_feed_ht_to_io();

void
exoredis_feed_io_to_ht();

void
exoredis_init_ht ();

void
exoredis_destroy_ht (exoredis_ht **hash_t);

void
exoredis_dump_ht (exoredis_ht **hash_t);

unsigned int
exoredis_hash_index (exoredis_ht **hash_t,
                     unsigned char *key,
                     int key_len);

void
exoredis_destroy_he (unsigned char *key,
                     int key_len);

void
exoredis_lookup_create_update_he (unsigned char * key, 
                           int key_len,
                           unsigned char * value,
                           int value_len,
                           exoredis_value_type type);
void *
exoredis_read_he (unsigned char *key,
                  int key_len,
                  unsigned char **value,
                  int *value_len,
                  exoredis_value_type *type);
void
exoredis_copy_to_update_hash_and_delete (unsigned char * key,
                                         int key_len,
                                         exoredis_hash_entry **he_input,
                                         exoredis_hash_entry **he_input_prev,
                                         int new_t_index);


exoredis_return_codes
exoredis_set_reset_bitoffset (unsigned char *key,
                              int key_len,
                              int offset,
                              int bitval,
                              unsigned char *orig_bitval);

exoredis_return_codes
exoredis_get_bitoffset (unsigned char *key,
                        int key_len,
                        int bit_offset,
                        unsigned char *orig_bitval);

exoredis_return_codes
exoredis_add_sortedset (unsigned char *key,
                        int key_len,
                        int *score,
                        unsigned char **value,
                        int *value_len,
                        int num);
#endif
