/**
 * Implementation of the SqlExecutor class defined in SqlExecutor.h.
 * This source file contains the logic for executing SQL statements,
 * specifically focusing on 'SELECT' and 'CREATE TABLE' queries. It
 * details how these statements are parsed and transformed into string
 * representations to simulate SQL queries execution.
 */

#include "SqlExecutor.h"
#include "SQLParser.h"
#include <string>
#include <sstream>
#include <iostream>

using namespace hsql;
SqlExecutor::SqlExecutor() {}

SqlExecutor::~SqlExecutor() {}

std::string SqlExecutor::execute(const SQLStatement *query)
{
    if (query->type() == kStmtSelect)
    {
        return handleSelect((const SelectStatement *)query);
    }
    else if (query->type() == kStmtCreate)
    {
        return handleCreate((const CreateStatement *)query);
    }
    return "The only handled queries are `SELECT` and `CREATE TABLE`";
}

std::string SqlExecutor::handleSelect(const SelectStatement *selectStmt)
{
    std::stringstream ss;

    ss << "SELECT ";
    if (selectStmt->selectDistinct)
    {
        ss << "DISTINCT ";
    }

    // Select list
    size_t count = selectStmt->selectList->size();
    for (size_t i = 0; i < count; ++i)
    {
        hsql::Expr *expr = selectStmt->selectList->at(i);
        handleExpression(expr, ss);

        // Add a comma and space after each element except the last one
        if (i < count - 1)
        {
            ss << ", ";
        }
    }

    // FROM Clause
    ss << " FROM ";
    handleTableRef(selectStmt->fromTable, ss);

    // WHERE Clause
    if (selectStmt->whereClause != NULL)
    {
        ss << " WHERE ";
        handleExpression(selectStmt->whereClause, ss);
    }

    return ss.str();
}

std::string SqlExecutor::handleCreate(const CreateStatement *createStmt)
{
    std::stringstream ss;
    ss << "CREATE TABLE " << createStmt->tableName << " (";
    size_t numColumns = createStmt->columns->size();
    for (size_t i = 0; i < numColumns; ++i)
    {
        ColumnDefinition *col = createStmt->columns->at(i);
        ss << columnDefinitionToString(col);

        // Add a comma after all but the last column
        if (i < numColumns - 1)
        {
            ss << ", ";
        }
    }
    ss << ")";
    return ss.str();
}

void SqlExecutor::handleTableRef(TableRef *table, std::stringstream &ss)
{
    switch (table->type)
    {
    case kTableName:
        ss << table->name;
        break;
    case kTableJoin:
        handleTableRef(table->join->left, ss);

        if (table->join->type == hsql::kJoinLeft)
        {
            ss << " LEFT";
        }
        else if (table->join->type == hsql::kJoinRight)
        {
            ss << " RIGHT";
        }
        ss << " JOIN ";

        handleTableRef(table->join->right, ss);

        ss << " ON ";

        handleExpression(table->join->condition, ss);
        break;
    case kTableCrossProduct:
        size_t count = table->list->size();
        for (size_t i = 0; i < count; ++i)
        {
            TableRef *tbl = table->list->at(i);
            handleTableRef(tbl, ss);

            if (i < count - 1)
            {
                ss << ", ";
            }
        }
        break;
    }
    if (table->alias != NULL)
    {
        ss << " AS " << table->alias;
    }
}

void SqlExecutor::handleExpression(Expr *expr, std::stringstream &ss)
{
    switch (expr->type)
    {
    case kExprStar:
        ss << "*";
        break;
    case kExprColumnRef:
        ss << (expr->hasTable() ? std::string(expr->table) + "." : "") << expr->name;
        break;
    case kExprLiteralFloat:
        ss << expr->fval;
        break;
    case kExprLiteralInt:
        ss << expr->ival;
        break;
    case kExprLiteralString:
        ss << expr->name;
        break;

    case kExprOperator:
        handleOperatorExpression(expr, ss);
        break;
    default:
        fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
        return;
    }
    if (expr->alias != NULL)
    {
        ss << " AS " << expr->alias;
    }
}

void SqlExecutor::handleOperatorExpression(Expr *expr, std::stringstream &ss)
{
    handleExpression(expr->expr, ss);
    if (expr == NULL)
    {
        ss << "null";
        return;
    }

    switch (expr->opType)
    {
    case Expr::SIMPLE_OP:
        ss << " " << expr->opChar << " ";
        break;
    case Expr::AND:
        ss << " AND ";
        break;
    case Expr::OR:
        ss << " OR ";
        break;
    case Expr::NOT:
        ss << " NOT ";
        break;
    default:
        ss << " " << expr->opType << " ";
        break;
    }
    if (expr->expr2 != NULL)
        handleExpression(expr->expr2, ss);
}

std::string SqlExecutor::columnDefinitionToString(const ColumnDefinition *col)
{
    std::string ret(col->name);
    switch (col->type)
    {
    case ColumnDefinition::DOUBLE:
        ret += " DOUBLE";
        break;
    case ColumnDefinition::INT:
        ret += " INT";
        break;
    case ColumnDefinition::TEXT:
        ret += " TEXT";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
}
