#ifndef EXOREDIS_HASH_H_
#define EXOREDIS_HASH_H_

/* To make the hashes work fast, adopt static length for buffers */
#define EXOREDIS_HASH_KEY_LEN 100

struct _exoredis_hash_entry {
    struct _exoredis_hash_entry *next;

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
exoredis_create_update_he (unsigned char * key, 
                           int key_len,
                           unsigned char * value,
                           int value_len);
void
exoredis_read_he (unsigned char * key,
                  int key_len);
void
exoredis_copy_to_update_hash_and_delete (unsigned char * key,
                                         int key_len,
                                         exoredis_hash_entry **he_input,
                                         exoredis_hash_entry **he_input_prev,
                                         int new_t_index);
#endif
