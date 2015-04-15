/*
 * exoredis_server.h:
 * This file contains declarations of util functions
 */

#ifndef EXOREDIS_SERVER_H
#define EXOREDIS_SERVER_H

void exoredis_handle_get(unsigned char *arg, int len);
void exoredis_handle_set(unsigned char *arg, int len);

void exoredis_handle_getbit(unsigned char *arg, int len);
void exoredis_handle_setbit(unsigned char *arg, int len);

void exoredis_handle_zadd (unsigned char *arg, int len);
void exoredis_handle_zrange (unsigned char *arg, int len);
void exoredis_handle_zcount (unsigned char *arg, int len);
void exoredis_handle_zcard (unsigned char *arg, int len);

void exoredis_handle_save (void);

#endif
