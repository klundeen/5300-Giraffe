CFLAGS = -std=c++11 -Wall -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
# -Wno-c++11-compat, We are not sure what these do
GLOBAL_PATH = /usr/local/db6
INCLUDE = $(GLOBAL_PATH)/include
LIB = $(GLOBAL_PATH)/lib

# All objects that need to be built to satisfy "sqlshell" target
OBJS       = sqlshell.o

# Catch all for .o targets, points to their cpp
%.o: %.cpp
	g++ -I$(INCLUDE) $(CFLAGS) -o "$@" "$<"

# Main build target, builds the corresponding object, compile command includes
# there are linkage statements for the db_cxx and sqlparser
sqlshell: $(OBJS)
	g++ -L$(LIB) -o $@ $< -ldb_cxx -lsqlparser

#Cleaning
clean:
	rm -f sqlshell.o sqlshell
