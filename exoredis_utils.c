#include "exoredis.h"
#include "exoredis_server.h"

#if 0
/*
 * exoredis_handle_get():
 * This function handles the REDIS protocol GET request
 * It looks up the DB file passed in as argument, and:
 * 1. If key exists, returns the value if the value is string. 
 * 2. If key exists, error is returned if value is not string
 * 3. If key does not exist, returns a bulk string nil.
 */
void exoredis_handle_get (char *arg)
{
    printf("GET Command: GET %s\n", arg);

    return;
}
#endif

void exoredis_handle_save(char *arg)
{
    printf("SAVE Command\n");
    return;
}
