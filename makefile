CC = gcc
DEPS = heap.h
CFLAGS = -g

all: sort show

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

show.o: show.c
	$(CC) -c -o $@ $< $(CFLAGS)

sort: sort.o heap.o
	$(CC) $(CFLAGS) -o sort sort.o heap.o

show: showdata.o
	$(CC) $(CFLAGS) -o show showdata.c

.PHONY: clean all

clean:
	rm -f sort show run_*.bin *.o 
