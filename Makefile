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

all: Search

 
libMapReduceFramework.o: libMapReduceFramework.cpp libMapReduceFramework.h
	$(CC) -c $^ -o $@

libMapReduceFramework.a: $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

Search: Search.o libMapReduceFramework.a
	$(CC) $^ $(LOADLIBES) -lMapReduceFramework -o $@ -lpthread

Search.o: Search.cpp
	$(CC) -c $^ -o $@

clean:
	$(RM) $(TARGETS) $(THREADLIB) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tara:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

jona:
	mv -f Search jonas/Test_496/Search; \
	mv -f libMapReduceFramework.a jonas/Framework/MapReduceFramework.a; \
	cd jonas; \
	./compile_suite; \
	./run_suite; \
	cd ..;
jonarun:
	cd jonas; \
	./run_suite; \
	cd ..;

val:
	valgrind --show-leak-kinds=all -v --leak-check=full search omeriscool /cs/usr/omer/hujios/Ex3/testush/uniqueFolder /cs/usr/omer/hujios/Ex3/testush/uniqueFolder1 /cs/usr/omer/hujios/Ex3/testush/uniqueFolder2 /cs/usr/omer/hujios/Ex3/testush/uniqueFolder3 /cs/usr/omer/hujios/Ex3/testush/uniqueFolder4

