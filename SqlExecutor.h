#include "SQLParser.h"
#include "string.h"

using namespace hsql;
class SqlExecutor
{
public:
    SqlExecutor();
    ~SqlExecutor();

    std::string execute(const SQLStatement *query);

private:
    std::string handleSelect(const SelectStatement *selectStmt);
    std::string handleCreate(const CreateStatement *createStmt);
    std::string columnDefinitionToString(const ColumnDefinition *col);
};
