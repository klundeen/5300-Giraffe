LIBS = -ldb_cxx -lsqlparser
SRC_DIR = src

all: sql5300

sql5300: sql5300.o SqlExecutor.o  
	g++ -L/usr/local/db6/lib -o $@ $^ $(LIBS)

sql5300.o: $(SRC_DIR)/sql5300.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

SqlExecutor.o: $(SRC_DIR)/SqlExecutor.cpp $(SRC_DIR)/SqlExecutor.h  
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean: 
	rm -f sql5300.o SqlExecutor.o sql5300
