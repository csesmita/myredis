#include "exoredis.h"
#include "exoredis_server.h"
#include "exoredis_hash.h"
#include <ctype.h>

/* Define prefix strings for the data types */
char prefix_string[MAX_DATA_TYPE] = {'+','-',':','$','*'};

#define TRAIL_STRING_LEN 2
char trail_string[TRAIL_STRING_LEN] = "\r\n";

#define SIMPLE_STRING_MAX_LEN 4
char simple_string[SIMPLE_STRING_MAX][SIMPLE_STRING_MAX_LEN] = {"OK "};

#define ERROR_STRING_MAX_LEN 10
char 
error_string[ERROR_STRING_MAX][ERROR_STRING_MAX_LEN] = {"ERR ", "WRONGTYPE "};


void exoredis_resp_error(char *msg,
                         exoredis_error_strings type)
{
    char buf[MAX_REQ_RESP_MSGLEN];
    char *ptr = buf;
    int buf_len = 0;

    memset(ptr, 0, MAX_REQ_RESP_MSGLEN);
    /* OK to use string operations since these are always simple strings */
    *ptr++ = prefix_string[ERROR_DATA_TYPE];
    buf_len++;
    memcpy(ptr, error_string[type], 
           strlen(error_string[type]));
    ptr += strlen(error_string[type]);
    buf_len += strlen(error_string[type]);
    memcpy(ptr, msg, strlen(msg));
    ptr += strlen(msg);
    buf_len += strlen(msg);
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_resp_ok(void)
{
    char buf[MAX_REQ_RESP_MSGLEN];
    char *ptr = buf;
    int buf_len = 0;

    memset(ptr, 0, MAX_REQ_RESP_MSGLEN);
    /* OK  to use string operations since these are always simple strings */
    *ptr++ = prefix_string[SIMPLE_STRING_DATA_TYPE];
    buf_len++;
    memcpy(ptr, simple_string[SIMPLE_STRING_OK], 
            strlen(simple_string[SIMPLE_STRING_OK]));

    ptr += strlen(simple_string[SIMPLE_STRING_OK]);
    buf_len += strlen(simple_string[SIMPLE_STRING_OK]);

    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_resp_integer (int val)
{
    unsigned char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    int buf_len = 0;
    int len_len = 0;

    memset(buf, 0, MAX_REQ_RESP_MSGLEN);
    unsigned char *ptr = buf;

    /* OK  to use string operations since these are always simple strings */
    *ptr++ = prefix_string[INTEGER_DATA_TYPE];
    buf_len++;
    len_len = sprintf((char *)ptr, "%d", val);

    ptr += len_len;
    buf_len += len_len;

    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;
    
    send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);
}

void exoredis_resp_bulk_string (unsigned char *msg,
                                int len,
                                unsigned char to_send,
                                unsigned char **buf,
                                int *buf_len)
{
    unsigned char *ptr = *buf;
    int len_len = 0;

    /* OK to use string operations since these are always simple strings */
    *ptr++ = prefix_string[BULK_STRING_DATA_TYPE];
    (*buf_len)++;
    len_len = sprintf((char *)ptr, "%d", len);
    ptr += len_len;
    (*buf_len) += len_len;
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    (*buf_len) += TRAIL_STRING_LEN;
    if (len) {
        memcpy(ptr, msg, len);
        ptr += len;
        (*buf_len) += len;
        memcpy(ptr, trail_string, TRAIL_STRING_LEN);
        ptr += TRAIL_STRING_LEN;
        (*buf_len) += TRAIL_STRING_LEN;
    }
    if (to_send) {
        send(exoredis_io.fd, *buf, *buf_len, MSG_DONTWAIT);
    }
    /* Modify incoming pointer to end of buffer */
    *buf = ptr;
}

void exoredis_resp_array (unsigned char *msg,
                          int len,
                          int size,
                          unsigned char to_send)
{
    unsigned char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    unsigned char *ptr = buf;
    int buf_len = 0;
    int len_len = 0;

    memset(buf, 0, MAX_REQ_RESP_MSGLEN);

    *ptr++ = prefix_string[ARRAY_DATA_TYPE];
    buf_len++;
    len_len = sprintf((char *)ptr, "%d", size);
    ptr += len_len;
    buf_len += len_len;
    memcpy(ptr, trail_string, TRAIL_STRING_LEN);
    ptr += TRAIL_STRING_LEN;
    buf_len += TRAIL_STRING_LEN;

    /* Msg already has the tail string */
    memcpy(ptr, msg, len);
    ptr += len;
    buf_len += len;

    if (to_send)
        send(exoredis_io.fd, buf, buf_len, MSG_DONTWAIT);

}

exoredis_return_codes
exoredis_parse_key_arg (unsigned char **key,
                        int *args_len,
                        int *key_len)
{
    *key_len = 0;
    unsigned char *ptr;

    /* Check for key value format */
    while (((**key) == ' ') && (*args_len > 0)) {
        (*key)++; (*args_len)--;
    }

    ptr = *key;
    while ((*ptr != ' ') && (*args_len > 0)) {
        ptr++; (*args_len)--; (*key_len)++;
    }

    return (*key_len == 0)? EXOREDIS_ARGS_MISSING: EXOREDIS_OK;
}

exoredis_return_codes
exoredis_parse_value_arg (unsigned char **value,
                          int *args_len,
                          int *value_len)
{
    unsigned char *ptr;
    *value_len = 0;

    while ((**value == ' ') && (*args_len > 0)) {
        (*value)++; (*args_len)--;
    }

    ptr = *value;
    while ((*ptr != ' ') && (*args_len > 0)) {
        ptr++; (*args_len)--; (*value_len)++;
    }
    return (*value_len == 0)? EXOREDIS_ARGS_MISSING: EXOREDIS_OK;
}

exoredis_return_codes
exoredis_parse_int_arg (unsigned char **bo,
                        int *args_len,
                        int *bo_value)
{
    long long value = 0;
    unsigned char *bo_start = NULL;

    while ((**bo == ' ') && (*args_len > 0)) {
        (*bo)++; (*args_len)--;
    }

    if (*args_len == 0) {
        return EXOREDIS_ARGS_MISSING;
    }

    bo_start = *bo;
    /* We have a non-space character */
    while (isdigit(**bo) && (*args_len) > 0) {
        (*bo)++; (*args_len)--;
    }

    if (*args_len != 0) {
        /* At this point only a space is expected */
        if (!isspace(**bo)) {
            /* Unexpected non-digit character */
            return EXOREDIS_BO_ARGS_INVALID;
        }
    }

    /* Convert value between bo_start and *bo to an integer */
    value = strtoll((char *)bo_start, (char **)bo, 10);

    if ((value < 0) || (value > UINT_MAX)) {
        /* Unexpected non-digit character */
        return EXOREDIS_BO_ARGS_INVALID;
    }
    *bo_value = (unsigned int)(value);

    return EXOREDIS_OK;
}


exoredis_return_codes
exoredis_parse_bit_arg (unsigned char **buf,
                        int *args_len,
                        unsigned char *bitset)
{
    while ((**buf == ' ') && (*args_len > 0)) {
        (*buf)++; (*args_len)--;
    }

    *bitset = (unsigned char)(atoi((char *)(*buf)));
    if (*bitset & ~1) {
        return EXOREDIS_BO_ARGS_INVALID;
    }
    (*buf)++;
    return (*args_len == 0)? EXOREDIS_ARGS_MISSING : EXOREDIS_OK;

}

exoredis_return_codes
exoredis_parse_optional_arg (unsigned char **buf,
                             int *args_len,
                             unsigned char *flags,
                             int *exp)
{
    *flags = 0;
    exp[0] = 0;
    exp[1] = 0;
    while(*args_len > 0 ) {
        while ((**buf == ' ') && (*args_len > 0)) {
            (*buf)++; (*args_len)--;
        }
        
        if (!strncpy((char *)(*buf), "EX", 2)) {
            /* Set seconds to expiry */
            (*buf) += 2;
            (*args_len) -= 2;
            *flags |= OPTION_EX;
            if(exoredis_parse_int_arg(buf, args_len, &exp[0]) != 
               EXOREDIS_OK) {
                return EXOREDIS_ERR;
            }
            if (exp[0] <= 0) {
                return EXOREDIS_BO_ARGS_INVALID;
            }
        } else if (!strncpy((char *)(*buf), "PX", 2)) {
            (*buf) += 2;
            (*args_len) -= 2;
            *flags |= OPTION_PX;
            /* Set milliseconds to expiry */
            if(exoredis_parse_int_arg(buf, args_len, &exp[1]) != 
               EXOREDIS_OK) {
                return EXOREDIS_ERR;
            }
            if (exp[1] <= 0) {
                return EXOREDIS_BO_ARGS_INVALID;
            }
        } else if (!strncpy((char *)(*buf), "NX", 2)) {
            (*buf) += 2;
            (*args_len) -= 2;
            *flags |= OPTION_NX;
            /* Only set if it does not exist */
        } else if (!strncpy((char *)(*buf), "XX", 2)) {
            (*buf) += 2;
            (*args_len) -= 2;
            /* Only set if it exists */
            *flags |= OPTION_XX;
        } else { 
            return EXOREDIS_ERR;
        }
    }
    return EXOREDIS_OK;

}

void
exoredis_handle_set (unsigned char *key,
                          int args_len)
{
    unsigned char *ptr = NULL, *options = NULL, flags = 0;
    int key_len = 0;
    int value_len = 0;
    int exp_val[2] = {0};
    exoredis_value_type type;
    exoredis_return_codes ret;
    unsigned char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    int buf_len = 0;


    /* Format : SET key string */
    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SET' command",
                            ERROR_STRING_ERR);
        return;
    }

    ptr = key + key_len;
    if (exoredis_parse_value_arg(&ptr, &args_len, &value_len) ==
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SET' command",
                            ERROR_STRING_ERR);
        return;
    }

    options = ptr + value_len;
    if ((ret = exoredis_parse_optional_arg(&options, &args_len, &flags, 
                                    exp_val)) == EXOREDIS_ERR) {
        exoredis_resp_error("syntax error", ERROR_STRING_ERR);
        return;
    } else if (ret == EXOREDIS_BO_ARGS_INVALID) {
        exoredis_resp_error("invalid expire time in set", ERROR_STRING_ERR);
        return;
    }
    type = ENCODING_VALUE_TYPE_STRING;
    if (flags != 0) {
       if ((flags & OPTION_EX) && (flags & OPTION_PX)) {
           type = ENCODING_VALUE_TYPE_STRING_SEC_EX_PX;
       } else if (flags & OPTION_EX) {
           type = ENCODING_VALUE_TYPE_STRING_SEC_EX;
       } else if (flags & OPTION_PX) {
           type = ENCODING_VALUE_TYPE_STRING_SEC_PX;
       }
    }
    /* Write the value into hash */
    if (exoredis_lookup_create_update_he(key, key_len, ptr, value_len, flags, 
                                         exp_val, type) == NULL) {
        ptr = buf;
        exoredis_resp_bulk_string(NULL, -1, 1, &ptr, &buf_len);
        return;
    }
    exoredis_resp_ok();
    return;
}

/*
 * exoredis_handle_get():
 * This function handles the REDIS protocol GET request
 * It looks up the DB file passed in as argument, and:
 * 1. If key exists, returns the value if the value is string. 
 * 2. If key exists, error is returned if value is not string
 * 3. If key does not exist, returns a bulk string nil.
 */

void exoredis_handle_get (unsigned char *key,
                          int args_len)
{
    int key_len = 0;
    int value_len = 0;
    unsigned char *value = NULL;
    exoredis_value_type type;
    unsigned char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    int buf_len = 0;
    unsigned char *ptr = buf;

    memset(buf, 0, MAX_REQ_RESP_MSGLEN);

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'GET' command",
                            ERROR_STRING_ERR);
        return;
    }

    value = key + key_len;
    if (exoredis_parse_value_arg(&value, &args_len, &value_len) ==
        EXOREDIS_OK) {
        /* Hmm we don't expect anything beyond the key */
        exoredis_resp_error("wrong number of arguments for 'GET' command",
                            ERROR_STRING_ERR);
        return;
    }

    /* Read the value from hash */
    exoredis_read_he(key, key_len, &value, &value_len, &type);

    if (exoredis_string_type_mismatch(type)) {
        /* Deallocate memory */
        if (value_len) {
            free(value);
        }
        exoredis_resp_error("Operation against a key holding"
                            " the wrong kind of value", ERROR_STRING_WRONGTYPE);
        return;
    }
    
    /* Send Bulk String */
    exoredis_resp_bulk_string(value, value_len, 1, &ptr, &buf_len);

    /* Deallocate memory */
    if (value_len) {
        free(value);
    }

    return;
}

void exoredis_handle_save (void)
{
    /* Dump the contents of the hash table into the DB File */
    exoredis_feed_ht_to_io();

    exoredis_resp_ok();
    
    return;
}

void exoredis_handle_setbit (unsigned char *key,
                             int args_len)
{
    int bo = 0;
    int key_len = 0;
    unsigned char bitset;
    exoredis_return_codes ret;
    unsigned char *bitpos;
    unsigned char *bitchar;
    unsigned char orig_val;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'SETBIT' command",
                            ERROR_STRING_ERR);
        return;
    }

    bitpos = key + key_len;
    if ((ret = exoredis_parse_int_arg(&bitpos, &args_len, &bo)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'SETBIT' command",
                                ERROR_STRING_ERR);
        } else {
            exoredis_resp_error("bit offset is not a integer or out of range",
                                ERROR_STRING_ERR);
        }
        return;
    }

    bitchar = bitpos;
    if ((ret = exoredis_parse_bit_arg(&bitchar, &args_len, &bitset)) !=
        EXOREDIS_OK) {
            if (ret == EXOREDIS_BO_ARGS_INVALID) {
                exoredis_resp_error("bit offset is not a integer or out of range",
                                    ERROR_STRING_ERR);
            } else {
                exoredis_resp_error("wrong number of arguments for 'SETBIT' command",
                                    ERROR_STRING_ERR);
            }
            return;
    }

    /* Set/Reset the bit at bitpos */

    exoredis_set_reset_bitoffset(key, key_len, bo, bitset, &orig_val);

    exoredis_resp_integer(orig_val);
    return;
}

void exoredis_handle_getbit (unsigned char *key,
                             int args_len)
{
    int bo = 0;
    int key_len = 0;
    exoredis_return_codes ret;
    unsigned char *bitpos;
    unsigned char *value;
    unsigned char orig_val;
    int value_len;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'GETBIT' command",
                            ERROR_STRING_ERR);
        return;
    }

    bitpos = key + key_len;
    if ((ret = exoredis_parse_int_arg(&bitpos, &args_len, &bo)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'GETBIT' command",
                                ERROR_STRING_ERR);
        } else {
            exoredis_resp_error("bit offset is not a integer or out of range",
                                ERROR_STRING_ERR);
        }
        return;
    }

    value = bitpos;
    if (exoredis_parse_value_arg(&value, &args_len, &value_len) ==
        EXOREDIS_OK) {
        /* Hmm we don't expect anything beyond the key */
        exoredis_resp_error("wrong number of arguments for 'GETBIT' command",
                            ERROR_STRING_ERR);
        return;
    }

    /* Get the bit at bitpos */
    ret = exoredis_get_bitoffset(key, key_len, bo, &orig_val);
    if (ret == EXOREDIS_WRONGTYPE) {
        exoredis_resp_error("Operation against a key holding"
                            " the wrong kind of value", ERROR_STRING_WRONGTYPE);
    }
    exoredis_resp_integer(orig_val);
    return;
}

void exoredis_handle_zadd (unsigned char *key,
                           int args_len)
{
    int score[MAX_REQ_RESP_INT_NUM];
    int key_len = 0;
    exoredis_return_codes ret;
    unsigned char *scorepos;
    unsigned char *value[MAX_REQ_RESP_INT_NUM];
    int value_len[MAX_REQ_RESP_INT_NUM];
    int i = 0;
    unsigned int added = 0;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'ZADD' command",
                            ERROR_STRING_ERR);
        return;
    }

    scorepos = key + key_len;
    while (i < MAX_REQ_RESP_INT_NUM) {
        if ((ret = exoredis_parse_int_arg(&scorepos, &args_len, &score[i])) !=
            EXOREDIS_OK) {
            if (i == 0 && (ret == EXOREDIS_ARGS_MISSING)) {
                exoredis_resp_error("wrong number of arguments for 'ZADD' command",
                                    ERROR_STRING_ERR);
                return;
            } else if (ret ==  EXOREDIS_BO_ARGS_INVALID) {
                exoredis_resp_error("value is not a valid integer",
                                    ERROR_STRING_ERR);
                return;
            }
            break;
        }

        value[i] = scorepos;
        if (exoredis_parse_value_arg(&value[i], &args_len, &value_len[i]) ==
            EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'ZADD' command",
                                ERROR_STRING_ERR);
            return;
        }
        scorepos = value[i] + value_len[i];
        i++;
    }

    ret = exoredis_add_sortedset(key, key_len, score, value, value_len, i,
                                 &added);
    exoredis_resp_integer(added);
    return;

}

void exoredis_handle_zcount (unsigned char *key,
                             int args_len)
{
    int key_len = 0;
    int min = 0, max = 0;
    exoredis_return_codes ret;
    unsigned char *minpos;
    unsigned char *maxpos;
    int count = 0;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'ZCOUNT' command",
                            ERROR_STRING_ERR);
        return;
    }

    minpos = key + key_len;
    if ((ret = exoredis_parse_int_arg(&minpos, &args_len, &min)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'ZCOUNT' command",
                                ERROR_STRING_ERR);
        } else if (ret ==  EXOREDIS_BO_ARGS_INVALID) {
            exoredis_resp_error("value is not a valid integer",
                                ERROR_STRING_ERR);
        }
        return;
    }

    maxpos = minpos;
    if ((ret = exoredis_parse_int_arg(&maxpos, &args_len, &max)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'ZCOUNT' command",
                                ERROR_STRING_ERR);
        } else if (ret ==  EXOREDIS_BO_ARGS_INVALID) {
            exoredis_resp_error("value is not a valid integer",
                                ERROR_STRING_ERR);
        }
        return;
    }

    ret = exoredis_count_sortedset(key, key_len, min, max, &count);
    exoredis_resp_integer(count);
    return;
}

void exoredis_handle_zrange (unsigned char *key,
                             int args_len)
{
    int key_len = 0;
    int min = 0, max = 0;
    exoredis_return_codes ret;
    unsigned char *minpos;
    unsigned char *maxpos;
    unsigned char withscore = 0;
    unsigned char *value;
    int value_len;
    unsigned char buf[MAX_REQ_RESP_MSGLEN];
    unsigned char *ptr = buf;
    int buf_len = 0;
    int size = 0;

    memset(ptr, 0, MAX_REQ_RESP_MSGLEN);

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'ZCOUNT' command",
                            ERROR_STRING_ERR);
        return;
    }

    minpos = key + key_len;
    if ((ret = exoredis_parse_int_arg(&minpos, &args_len, &min)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'ZRANGE' command",
                                ERROR_STRING_ERR);
        } else if (ret ==  EXOREDIS_BO_ARGS_INVALID) {
            exoredis_resp_error("value is not a valid integer",
                                ERROR_STRING_ERR);
        }
        return;
    }

    maxpos = minpos;
    if ((ret = exoredis_parse_int_arg(&maxpos, &args_len, &max)) !=
        EXOREDIS_OK) {
        if (ret == EXOREDIS_ARGS_MISSING) {
            exoredis_resp_error("wrong number of arguments for 'ZRANGE' command",
                                ERROR_STRING_ERR);
        } else if (ret ==  EXOREDIS_BO_ARGS_INVALID) {
            exoredis_resp_error("value is not a valid integer",
                                ERROR_STRING_ERR);
        }
        return;
    }
 
    value = maxpos;
    if ((ret = exoredis_parse_value_arg(&value, &args_len, &value_len)) !=
        EXOREDIS_ARGS_MISSING) {
        /* Optional argument provided */
        if (!memcmp(value, "withscores", 10)) {
            withscore = 1;
        } else {
            exoredis_resp_error("syntax error", ERROR_STRING_ERR);
            return;
        }
    }

    ret = exoredis_range_sortedset(key, key_len, min, max, withscore,
                                   &ptr, &buf_len, &size, FALSE);
    exoredis_resp_array(buf,buf_len, size, 1);
    return;

}

void exoredis_handle_zcard (unsigned char *key,
                            int args_len)
{
    int card;
    int key_len = 0;
    unsigned char *value;
    int value_len;

    if (exoredis_parse_key_arg(&key, &args_len, &key_len) == 
         EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'ZCARD' command",
                            ERROR_STRING_ERR);
        return;
    }

    value = key + key_len;
    if (exoredis_parse_value_arg(&value, &args_len, &value_len) !=
        EXOREDIS_ARGS_MISSING) {
        exoredis_resp_error("wrong number of arguments for 'ZCARD' command",
                            ERROR_STRING_ERR);
        return;
    }

    exoredis_card_sortedset(key, key_len, &card);
    exoredis_resp_integer(card);
}
