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
    return "other";
}

std::string SqlExecutor::handleSelect(const SelectStatement *selectStmt)
{
    std::stringstream ss;

    ss << "SELECT ";
    if (selectStmt->selectDistinct)
    {
        ss << "DISTINCT ";
    }

    // TODO:cols

    // FROM clause
    if (selectStmt->fromTable)
    {
        ss << " FROM " << selectStmt->fromTable->name; 
    }

    if (selectStmt->fromTable && selectStmt->fromTable->join)
    {
        const JoinDefinition *join = selectStmt->fromTable->join;
        ss << " LEFT JOIN " << join->right->name;
        ss << " ON " << join->condition->expr->name << " = " << join->condition->expr2->name;
    }

    return ss.str();
}

std::string SqlExecutor::handleCreate(const CreateStatement *createStmt)
{
    std::stringstream ss;
    ss << "CREATE TABLE " << createStmt->tableName << "(";
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

//  void printExpression(Expr* expr, std::stringstream& ss) {
// switch (expr->type) {
// case kExprStar:
//     ss << "*";
//     break;
// case kExprColumnRef:
//     if (expr->hasTable()) {
//         ss << expr->table << ".";
//     }
//     ss << expr->name;
//     break;
// // case kExprTableColumnRef: inprint(expr->table, expr->name, numIndent); break;
// case kExprLiteralFloat:
//   inprint(expr->fval, numIndent);
//   break;
// case kExprLiteralInt:
//   inprint(expr->ival, numIndent);
//   break;
// case kExprLiteralString:
//   inprint(expr->name, numIndent);
//   break;
// case kExprFunctionRef:
//   inprint(expr->name, numIndent);
//   inprint(expr->expr->name, numIndent + 1);
//   break;
// case kExprOperator:
//   printOperatorExpression(expr, numIndent);
//   break;
// default:
//   fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
//   return;
// }
// if (expr->alias != NULL) {
//   inprint("Alias", numIndent + 1);
//   inprint(expr->alias, numIndent + 2);
// }
//   }

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
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