/*
 * exoredis_server.c:
 * The main server-side logic for Exoredis server
 */

#include "exoredis.h"
#include "exoredis_server.h"
#include "exordb.h"
#include "exoredis_hash.h"

/*
 * exoredis_str_to_cmd:
 * Interpretes incoming request to figure out the command
 */
exoredis_supported_cmds exoredis_str_to_cmd(char *cmd,
                                            int command_len)
{
    if (!memcmp(cmd, "GET", command_len)) {
        return EXOREDIS_CMD_GET;
    }
    
    if (!memcmp(cmd, "SET", command_len)) {
        return EXOREDIS_CMD_SET;
    }
    
    if (!memcmp(cmd, "GETBIT", command_len)) {
        return EXOREDIS_CMD_GETBIT;
    }

    if (!memcmp(cmd, "SETBIT", command_len)) {
        return EXOREDIS_CMD_SETBIT;
    }

    if (!memcmp(cmd, "ZADD", command_len)) {
        return EXOREDIS_CMD_ZADD;
    }

    if (!memcmp(cmd, "ZCOUNT", command_len)) {
        return EXOREDIS_CMD_ZCOUNT;
    }

    if (!memcmp(cmd, "ZCARD", command_len)) {
        return EXOREDIS_CMD_ZCARD;
    }

    if (!memcmp(cmd, "ZRANGE", command_len)) {
        return EXOREDIS_CMD_ZRANGE;
    }

    if (!memcmp(cmd, "SAVE", command_len)) {
        return EXOREDIS_CMD_SAVE;
    }

    return EXOREDIS_CMD_MAX;
}

/*
 * exoredis_process_request:
 * Function that decides the type of request sent by the client
 * It then invokes the appropriate handler
 */
void exoredis_process_request(unsigned char *buf,
                              int read_len)
{
    unsigned char *ptr = buf;
    exoredis_supported_cmds cmd;
    int command_len = 0;
    int args_len = 0;
    
    /* Split buf into command arguments */
    while ((*ptr != ' ') && (command_len < read_len)) {
        ptr++; command_len++;
    }

    cmd = exoredis_str_to_cmd((char *)(buf), command_len);

    if(cmd == EXOREDIS_CMD_MAX) {
        printf("Unrecognized command %s", buf);
        return;
    }
    args_len = read_len - command_len;

    while ((*ptr == ' ') && (args_len > 0)) {
        ptr++; args_len--;
    }

    if (args_len > 0) {
        buf = ptr;
    }

    switch(cmd) {
        case EXOREDIS_CMD_GET:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_get(buf, args_len);

        case EXOREDIS_CMD_SET:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_set(buf, args_len);

        case EXOREDIS_CMD_SETBIT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_setbit(buf, args_len);

        case EXOREDIS_CMD_GETBIT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_getbit(buf, args_len);

        case EXOREDIS_CMD_ZADD:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zadd(buf, args_len);

        case EXOREDIS_CMD_ZCOUNT:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zcount(buf, args_len);

        case EXOREDIS_CMD_ZCARD:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zcard(buf, args_len);

        case EXOREDIS_CMD_ZRANGE:
            printf("Arguments to command %s\n", buf);
            return exoredis_handle_zrange(buf, args_len);

        case EXOREDIS_CMD_SAVE:
            if (command_len < read_len) {
                printf("SAVE doesn't take arguments\n");
            }
            return exoredis_handle_save();

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
    unsigned char buf[MAX_REQ_RESP_MSGLEN + 1] = "\0";
    unsigned char *bufptr = buf;
    int fd = exoredis_io.fd;
    int read_len = 0;

    /* Initialize hash trees */
    exoredis_init_ht();

    while (1) {
        memset(buf, 0, MAX_REQ_RESP_MSGLEN + 1);

        /* Read request from this client */
        read_len = read(fd, buf, MAX_REQ_RESP_MSGLEN);

        /* Prune the trailing /r/n */
        read_len -= 2;

        /* Prune leading white-spaces */
        bufptr = buf;
        while ((*bufptr == ' ') && (read_len > 0)) {
            bufptr++;
            read_len--;
        }

        if (read_len == 0) {
            /* All spaces */
            continue;
        }

        exoredis_process_request(bufptr, read_len);
    }
}

void
exoredis_io_init (void)
{
   exoredis_io.fd = -1;
   exoredis_io.dbfp = NULL;
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

    if((fp = fopen(argv[1], "rb")) == NULL) {
        printf("Error opening DB file\n");
        return(-1);
    }

    exoredis_io_init();
    exoredis_io.dbfp = fp;
    strncpy(exoredis_io.filePath, argv[1], strlen(argv[1]));

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

    /* Just before heading over to clients, load the DB file into hash table */
    //exoredis_feed_io_to_ht();

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
            fclose(fp);
            close(new_fd);
        }
    }

    /* Clean up */
    close(listen_sock);
    return 0;
}
