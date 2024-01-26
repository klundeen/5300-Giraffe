LIBS = -ldb_cxx -lsqlparser
SRC_DIR = src
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS = sql5300.o heap_storage.o SqlExecutor.o

all: sql5300

sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $^ $(LIBS)

sql5300.o: $(SRC_DIR)/sql5300.cpp
	g++ -I$(INCLUDE_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

SqlExecutor.o: $(SRC_DIR)/SqlExecutor.cpp $(SRC_DIR)/SqlExecutor.h
	g++ -I$(INCLUDE_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

heap_storage.o: $(SRC_DIR)/heap_storage.cpp $(SRC_DIR)/heap_storage.h $(SRC_DIR)/storage_engine.h
	g++ -I$(INCLUDE_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean:
	rm -f sql5300.o SqlExecutor.o heap_storage.o sql5300
