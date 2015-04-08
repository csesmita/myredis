/*
 * exoredis_server.h:
 * This file contains declarations of util functions
 */

#ifndef EXOREDIS_SERVER_H
#define EXOREDIS_SERVER_H

void exoredis_handle_get(unsigned char *arg, int len);

void exoredis_handle_set(unsigned char *arg, int len);

#if 0
 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

 void exoredis_handle_get(int fd, struct sockaddr *client_addr, FILE *fp,
                          char *arg);

#endif
void exoredis_handle_save (void);

#endif
