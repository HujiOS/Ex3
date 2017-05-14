CC=g++
RANLIB=ranlib

LIBSRC=MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h
LIBOBJ=MapReduceFramework.o

INCS=-I.
CFLAGS = -Wall -g $(INCS)
LOADLIBES = -L./ 
LFLAGS = -o


THREADLIB = libuthreads.a

TAR=tar
TARFLAGS=-cvf
TARNAME=ex2.tar
TARSRCS=$(LIBSRC) Makefile README

all: search

 
libMapReduceFramework.o: libMapReduceFramework.cpp libMapReduceFramework.h
	$(CC) -c $^ -o $@

libMapReduceFramework.a: $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

search: Search.o libMapReduceFramework.a
	$(CC) $^ $(LOADLIBES) -lMapReduceFramework -o $@ -lpthread

Search.o: Search.cpp
	$(CC) -c $^ -o $@

clean:
	$(RM) $(TARGETS) $(THREADLIB) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tara:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

ctest:
	cp -f ex2.tar ex2sanity

rtest:
	python3 ex2sanity/test.py

val:
	valgrind --show-leak-kinds=all --leak-check=full test

