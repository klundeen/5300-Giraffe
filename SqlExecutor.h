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
    void handleTableRef(TableRef *table, std::stringstream &ss);
    std::string columnDefinitionToString(const ColumnDefinition *col);
    void handleExpression(Expr *expr, std::stringstream &ss);
    void handleOperatorExpression(Expr *expr, std::stringstream &ss);
};
