TARGET = exoredis_server
CFLAGS = -g -Wall
all: ${TARGET}

${TARGET}: 
	gcc ${CFLAGS} -o ${TARGET} exoredis_server.c \
    exoredis_hash.c \
    exoredis_utils.c

clean:
	rm -rf ${TARGET}
