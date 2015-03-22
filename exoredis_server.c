/*
 * exoredis_server.c:
 * The main server-side logic for Exoredis server
 */

#include "exoredis.h"
#include "exoredis_server.h"
#include "exordb.h"

/*
 * exoredis_str_to_cmd:
 * Interpretes incoming request to figure out the command
 */
exoredis_supported_cmds exoredis_str_to_cmd(char *cmd)
{
    if (!strncmp(cmd, "GET", strlen("GET"))) {
        return EXOREDIS_CMD_GET;
    }
    
    if (!strncmp(cmd, "SET", strlen("SET"))) {
        return EXOREDIS_CMD_SET;
    }
    
    if (!strncmp(cmd, "GETBIT", strlen("GETBIT"))) {
        return EXOREDIS_CMD_GETBIT;
    }

    if (!strncmp(cmd, "SETBIT", strlen("SETBIT"))) {
        return EXOREDIS_CMD_SETBIT;
    }

    if (!strncmp(cmd, "ZADD", strlen("ZADD"))) {
        return EXOREDIS_CMD_ZADD;
    }

    if (!strncmp(cmd, "ZCOUNT", strlen("ZCOUNT"))) {
        return EXOREDIS_CMD_ZCOUNT;
    }

    if (!strncmp(cmd, "ZCARD", strlen("ZCARD"))) {
        return EXOREDIS_CMD_ZCARD;
    }

    if (!strncmp(cmd, "ZRANGE", strlen("ZRANGE"))) {
        return EXOREDIS_CMD_ZRANGE;
    }

    if (!strncmp(cmd, "SAVE", strlen("SAVE"))) {
        return EXOREDIS_CMD_SAVE;
    }

    return EXOREDIS_CMD_MAX;
}

/*
 * exoredis_process_request:
 * Function that decides the type of request sent by the client
 * It then invokes the appropriate handler
 */
void exoredis_process_request(char *buf)
{
    char *ptr = buf;
    exoredis_supported_cmds cmd;
    
    /* Split buf into command arguments */
    while (*++ptr != ' ');
    *ptr = '\0';

    cmd = exoredis_str_to_cmd(buf);

    if(cmd == EXOREDIS_CMD_MAX) {
        printf("Unrecognized command %s", buf);
        return;
    }
    buf = ++ptr;

    switch(cmd) {
#if 0
        case EXOREDIS_CMD_GET:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_get(buf);

        case EXOREDIS_CMD_SET:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_set();

        case EXOREDIS_CMD_GETBIT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_getbit();

        case EXOREDIS_CMD_SETBIT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_setbit();

        case EXOREDIS_CMD_ZADD:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zadd();

        case EXOREDIS_CMD_ZCOUNT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zcount();

        case EXOREDIS_CMD_ZCARD:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zcard();

        case EXOREDIS_CMD_ZRANGE:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zrange();

#endif
        case EXOREDIS_CMD_SAVE:
            return exoredis_handle_save(buf);

        default:
            break;
    }
    
}


/*
 * exoredis_handle_client_req():
 * Function to handle incoming client request
 */
void exoredis_handle_client_req(void)
{
    char buf[MAX_REQ_RESP_MSGLEN] = "\0";
    char *bufptr = buf;
    unsigned templen = 0;
    int fd = exoredis_io.fd;

    while (1) {
        memset(buf, 0, MAX_REQ_RESP_MSGLEN);

        /* Read request from this client */
        read(fd, buf, MAX_REQ_RESP_MSGLEN);

        /* Prune leading white-spaces */
        while ((*bufptr == ' ') && templen < MAX_REQ_RESP_MSGLEN) {
            bufptr++;
            templen++;
        }

        if (templen == MAX_REQ_RESP_MSGLEN) {
            /* All spaces */
            continue;
        }

        printf("Client sends %s\n",bufptr);

        exoredis_process_request(bufptr);
    }
}

void
exoredis_io_init (oid)
{
   exoredis_io.fd = -1;
   exoredis_io.storage.file.fp = NULL;
   exoredis_io.storage.buffer.buf = NULL;
   exoredis_io.storage.buffer.offset = NULL;
}

int main(int argc, char *argv[])
{
    int listen_sock = 0, new_fd = 0;
    socklen_t sin_size;
    struct sockaddr_in server_addr;
    struct sockaddr client_addr;
    char addr_str[INET_ADDRSTRLEN] = "\0";
    FILE *fp = NULL;


    /* Take in the programme and filename */
    if (argc != 2) {
        printf("Please invoke exoredis_server with DB filename only\n");
        return(-1);
    }

    if((fp = fopen(argv[1], "a+")) == NULL) {
        printf("Error opening DB file\n");
        return(-1);
    }

    exoredis_io_init();
    exoredis_io.storage.file.fp = fp;

    if((listen_sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        printf("Server socket problem\n");
        return(-1);
    }

    printf("Server socket FD %d\n", listen_sock);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(EXOREDIS_SERVER_TCP_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bzero(&(server_addr.sin_zero),8);

    /* Bind the server to the specified address */
    bind(listen_sock, (struct sockaddr *)&server_addr, sizeof(struct sockaddr));

    listen(listen_sock, EXOREDIS_MAX_SERVER_CONN);

    printf("Server waiting for client requests on port %d\n",
           EXOREDIS_SERVER_TCP_PORT);

    /* Now wait infinitely for client to send requests */
    while(1) {
        sin_size = sizeof(client_addr);
        new_fd = accept(listen_sock, (struct sockaddr *)&client_addr, &sin_size);
        if (new_fd == -1) {
            /* Error accepting new request */
            perror("Error accepting new request");
            continue;
        }

        inet_ntop(AF_INET, &((struct sockaddr_in*)&client_addr)->sin_addr,
            addr_str, sizeof addr_str);
        printf("server: got connection from %s:%d\n", addr_str,
               ((struct sockaddr_in *)(&client_addr))->sin_port);

        /* Fork a child process to take care of this request */
        if (fork() == 0) {
            close(listen_sock);
            /* EXOREDIS PROTOCOL HANDLING */
            /* Handle incoming requests from this client */

            exoredis_io.fd = new_fd;
            exoredis_handle_client_req();
            close(new_fd); /* ? */
            exit(0);
        } else {
            /* Parent */
            close(new_fd);
        }
    }

    /* Clean up */
    close(listen_sock);
    return 0;
}
