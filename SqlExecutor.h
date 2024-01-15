#include "SQLParser.h"
#include "string.h"

using namespace hsql;
class SqlExecutor
{
public:
    SqlExecutor();
    ~SqlExecutor();

    /**
     * Executes the given SQL statement (currently it just parses it to the query statement).
     * @param query Pointer to the SQLStatement to be executed.
     * @return A string representation of the result of the execution.
     */
    std::string execute(const SQLStatement *query);

private:
    /**
     * Handles the SELECT statement.
     * @param selectStmt Pointer to the SelectStatement to be handled.
     * @return A string representation of the SELECT query.
     */
    std::string handleSelect(const SelectStatement *selectStmt);

    /**
     * Handles the CREATE TABLE statement.
     * @param createStmt Pointer to the CreateStatement to be handled.
     * @return A string representation of the CREATE query.
     */
    std::string handleCreate(const CreateStatement *createStmt);

    /**
     * Processes a TableRef and appends the corresponding SQL to the stringstream.
     * This function handles JOINS, CROSS PRODUCT and ALIASES
     * @param table Pointer to the TableRef to be processed.
     * @param ss Reference to the stringstream where SQL is appended.
     */
    void handleTableRef(TableRef *table, std::stringstream &ss);

    /**
     * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
     * @param col  column definition to unparse
     * @return     SQL equivalent to *col
     */
    std::string columnDefinitionToString(const ColumnDefinition *col);

    /**
     * Processes an Expression and appends the corresponding SQL to the stringstream.
     * @param expr Pointer to the Expr to be processed.
     * @param ss Reference to the stringstream where SQL is appended.
     */
    void handleExpression(Expr *expr, std::stringstream &ss);

    /**
     * Processes an operator expression and appends the corresponding SQL to the stringstream.
     * @param expr Pointer to the Expr representing the operator to be processed.
     * @param ss Reference to the stringstream where SQL is appended.
     */
    void handleOperatorExpression(Expr *expr, std::stringstream &ss);
};
