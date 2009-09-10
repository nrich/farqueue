CC=g++
CFLAGS=-g -O2 -D_GNU_SOURCE
RM=rm -f
LIBS=-levent -lsqlite3
OUT=farqueue

LDFLAGS= $(LIBS)

OBJS = farqueue.o

all: farqueue

clean:
	$(RM) $(OBJS) $(OUT)

luatorrent: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $(OUT) $(LDFLAGS)

farqueue.o: src/farqueue.c
	$(CC) $(CFLAGS) -c -o $@ $<

.PHONY: all
