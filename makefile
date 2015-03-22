TARGET = exoredis_server
CFLAGS = -g -Wall
all: ${TARGET}

${TARGET}: ${TARGET}.c exoredis_utils.c exoredis_hash.c
	gcc ${CFLAGS} -o ${TARGET} ${TARGET}.c  exoredis_utils.c exoredis_hash.c

clean:
	rm -rf ${TARGET}
