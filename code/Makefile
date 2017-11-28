CC=clang
CFLAGS=-I. -g -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse -Wall
LIBS = -luuid -lfuse -pthread -lm
DEPS = myfs.h unqlite.h
OBJ = unqlite.o

TARGET1 = myfs

all: $(TARGET1) 

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

$(TARGET1): $(TARGET1).o $(OBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

.PHONY: clean unmount mount

clean: unmount
	rm -f *.o *~ core myfs.db myfs.log $(TARGET1)

unmount:
	-fusermount -u test

mount: unmount
	./myfs test