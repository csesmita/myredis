#ifndef EXOREDIS_HASH_H_
#define EXOREDIS_HASH_H

struct _exoredis_hash_entry {
    unsigned int key;
    unsigned int value;
    struct _exoredis_hash_entry *next;
};

typedef struct _exoredis_hash_entry exoredis_hash_entry;

struct _exoredis_ht {
    unsigned size;
    exoredis_hash_entry **table;
};

typedef struct _exoredis_ht exoredis_ht;

#define EXOREDIS_HASH_SEED 257

#define HASH_TEST_MODE 1

#endif
