#include <string>
#include "SQLParser.h"

using namespace hsql;

class SqlExecutor {
public:
    SqlExecutor(); 
    ~SqlExecutor(); 

    void execute(const SQLParserResult query); 

private:
};

