CC := cc
CFLAGS := -std=c11 -Wall -Wextra -pedantic -O2 -pthread
LDFLAGS := -pthread

BINARIES := name_server storage_server client

all: $(BINARIES)

name_server: name_server.c protocol.h
	$(CC) $(CFLAGS) -o $@ name_server.c $(LDFLAGS)

storage_server: storage_server.c protocol.h
	$(CC) $(CFLAGS) -o $@ storage_server.c $(LDFLAGS)

client: client.c protocol.h
	$(CC) $(CFLAGS) -o $@ client.c $(LDFLAGS)

clean:
	rm -f $(BINARIES) nm.log ss.log

.PHONY: all clean