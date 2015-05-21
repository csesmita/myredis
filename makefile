TARGET = exoredis_server
CFLAGS = -g -Wall
all: ${TARGET}

${TARGET}: ${TARGET}.c exoredis_hash.c exoredis_utils.c
	gcc ${CFLAGS} -o ${TARGET} ${TARGET}.c  exoredis_hash.c exoredis_utils.c 

clean:
	rm -rf ${TARGET}
