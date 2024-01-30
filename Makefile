LIBS = -ldb_cxx -lsqlparser
SRC_DIR = src
COURSE = /usr/local/db6
INCLUDE_DIR = ./include
LIB_DIR = $(COURSE)/lib

# List of all the compiled object files needed to build the sql5300 executable
OBJS = sql5300.o heap_storage.o SqlExecutor.o

all: sql5300

sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $^ $(LIBS)

sql5300.o: $(SRC_DIR)/sql5300.cpp
	g++ -I$(INCLUDE_DIR) -I$(COURSE)/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

SqlExecutor.o: $(SRC_DIR)/SqlExecutor.cpp $(INCLUDE_DIR)/SqlExecutor.h
	g++ -I$(INCLUDE_DIR) -I$(COURSE)/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

heap_storage.o: $(SRC_DIR)/heap_storage.cpp $(INCLUDE_DIR)/heap_storage.h $(INCLUDE_DIR)/storage_engine.h
	g++ -I$(INCLUDE_DIR) -I$(COURSE)/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean:
	rm -f sql5300.o SqlExecutor.o heap_storage.o sql5300
