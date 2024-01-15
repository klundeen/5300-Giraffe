LIBS = -ldb_cxx -lsqlparser
SRC_DIR = src
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

all: sql5300

sql5300: sql5300.o SqlExecutor.o  
	g++ -L$(LIB_DIR) -o $@ $^ $(LIBS)

sql5300.o: $(SRC_DIR)/sql5300.cpp
	g++ -I$(INCLUDE_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

SqlExecutor.o: $(SRC_DIR)/SqlExecutor.cpp $(SRC_DIR)/SqlExecutor.h  
	g++ -I$(INCLUDE_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean: 
	rm -f sql5300.o SqlExecutor.o sql5300
