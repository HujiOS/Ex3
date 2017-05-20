CC=g++
RANLIB=ranlib

LIBSRC=MapReduceClient.h MapReduceFramework.cpp MapReduceFramework.h
LIBOBJ=MapReduceFramework.o
EXTRADELETE=libMapReduceFramework.a .MapReduceFramework.log Search.o

INCS=-I.
CFLAGS = -Wall -g $(INCS)
LOADLIBES = -L./ 
LFLAGS = -o

TAR=tar
TARFLAGS=-cvf
TARNAME=ex2.tar
TARSRCS=$(LIBSRC) Makefile README Search.cpp

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
	$(RM) $(TARGETS) $(OBJ) $(LIBOBJ) $(EXTRADELETE) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

mytest:
	./Search omeriscool /cs/usr/omer/hujios/Ex3/testush/u0 /cs/usr/omer/hujios/Ex3/testush/u1 /cs/usr/omer/hujios/Ex3/testush/u2 /cs/usr/omer/hujios/Ex3/testush/u3 /cs/usr/omer/hujios/Ex3/testush/u4 /cs/usr/omer/hujios/Ex3/testush/u5 /cs/usr/omer/hujios/Ex3/testush/u6 /cs/usr/omer/hujios/Ex3/testush/u7 /cs/usr/omer/hujios/Ex3/testush/u8 /cs/usr/omer/hujios/Ex3/testush/u9 /cs/usr/omer/hujios/Ex3/testush/u10 /cs/usr/omer/hujios/Ex3/testush/u11 /cs/usr/omer/hujios/Ex3/testush/u12 /cs/usr/omer/hujios/Ex3/testush/u13 /cs/usr/omer/hujios/Ex3/testush/u14 /cs/usr/omer/hujios/Ex3/testush/u15 /cs/usr/omer/hujios/Ex3/testush/u16 /cs/usr/omer/hujios/Ex3/testush/u17 /cs/usr/omer/hujios/Ex3/testush/u18 /cs/usr/omer/hujios/Ex3/testush/u19 /cs/usr/omer/hujios/Ex3/testush/u20 /cs/usr/omer/hujios/Ex3/testush/u21 /cs/usr/omer/hujios/Ex3/testush/u22 /cs/usr/omer/hujios/Ex3/testush/u23 /cs/usr/omer/hujios/Ex3/testush/u24 /cs/usr/omer/hujios/Ex3/testush/u25 /cs/usr/omer/hujios/Ex3/testush/u26 /cs/usr/omer/hujios/Ex3/testush/u27 /cs/usr/omer/hujios/Ex3/testush/u28 /cs/usr/omer/hujios/Ex3/testush/u29 /cs/usr/omer/hujios/Ex3/testush/u30 /cs/usr/omer/hujios/Ex3/testush/u31 /cs/usr/omer/hujios/Ex3/testush/u32 /cs/usr/omer/hujios/Ex3/testush/u33 /cs/usr/omer/hujios/Ex3/testush/u34 /cs/usr/omer/hujios/Ex3/testush/u35 /cs/usr/omer/hujios/Ex3/testush/u36 /cs/usr/omer/hujios/Ex3/testush/u37 /cs/usr/omer/hujios/Ex3/testush/u38 /cs/usr/omer/hujios/Ex3/testush/u39 /cs/usr/omer/hujios/Ex3/testush/u40 /cs/usr/omer/hujios/Ex3/testush/u41 /cs/usr/omer/hujios/Ex3/testush/u42 /cs/usr/omer/hujios/Ex3/testush/u43 /cs/usr/omer/hujios/Ex3/testush/u44 /cs/usr/omer/hujios/Ex3/testush/u45 /cs/usr/omer/hujios/Ex3/testush/u46 /cs/usr/omer/hujios/Ex3/testush/u47 /cs/usr/omer/hujios/Ex3/testush/u48 /cs/usr/omer/hujios/Ex3/testush/u49 /cs/usr/omer/hujios/Ex3/testush/u50 /cs/usr/omer/hujios/Ex3/testush/u51 /cs/usr/omer/hujios/Ex3/testush/u52 /cs/usr/omer/hujios/Ex3/testush/u53 /cs/usr/omer/hujios/Ex3/testush/u54 /cs/usr/omer/hujios/Ex3/testush/u55 /cs/usr/omer/hujios/Ex3/testush/u56 /cs/usr/omer/hujios/Ex3/testush/u57 /cs/usr/omer/hujios/Ex3/testush/u58 /cs/usr/omer/hujios/Ex3/testush/u59 /cs/usr/omer/hujios/Ex3/testush/u60 /cs/usr/omer/hujios/Ex3/testush/u61 /cs/usr/omer/hujios/Ex3/testush/u62 /cs/usr/omer/hujios/Ex3/testush/u63 /cs/usr/omer/hujios/Ex3/testush/u64 /cs/usr/omer/hujios/Ex3/testush/u65 /cs/usr/omer/hujios/Ex3/testush/u66 /cs/usr/omer/hujios/Ex3/testush/u67 /cs/usr/omer/hujios/Ex3/testush/u68 /cs/usr/omer/hujios/Ex3/testush/u69 /cs/usr/omer/hujios/Ex3/testush/u70 /cs/usr/omer/hujios/Ex3/testush/u71 /cs/usr/omer/hujios/Ex3/testush/u72 /cs/usr/omer/hujios/Ex3/testush/u73 /cs/usr/omer/hujios/Ex3/testush/u74 /cs/usr/omer/hujios/Ex3/testush/u75 /cs/usr/omer/hujios/Ex3/testush/u76 /cs/usr/omer/hujios/Ex3/testush/u77 /cs/usr/omer/hujios/Ex3/testush/u78 /cs/usr/omer/hujios/Ex3/testush/u79 /cs/usr/omer/hujios/Ex3/testush/u80 /cs/usr/omer/hujios/Ex3/testush/u81 /cs/usr/omer/hujios/Ex3/testush/u82 /cs/usr/omer/hujios/Ex3/testush/u83 /cs/usr/omer/hujios/Ex3/testush/u84 /cs/usr/omer/hujios/Ex3/testush/u85 /cs/usr/omer/hujios/Ex3/testush/u86 /cs/usr/omer/hujios/Ex3/testush/u87 /cs/usr/omer/hujios/Ex3/testush/u88 /cs/usr/omer/hujios/Ex3/testush/u89 /cs/usr/omer/hujios/Ex3/testush/u90 /cs/usr/omer/hujios/Ex3/testush/u91 /cs/usr/omer/hujios/Ex3/testush/u92 /cs/usr/omer/hujios/Ex3/testush/u93 /cs/usr/omer/hujios/Ex3/testush/u94 /cs/usr/omer/hujios/Ex3/testush/u95 /cs/usr/omer/hujios/Ex3/testush/u96 /cs/usr/omer/hujios/Ex3/testush/u97 /cs/usr/omer/hujios/Ex3/testush/u98 /cs/usr/omer/hujios/Ex3/testush/u99

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
