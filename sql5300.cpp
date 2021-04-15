#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"


// CREATE A DIRECTORY IN YOUR HOME DIR ~/cpsc5300/data before running this
const char *SQL_DB = "sqldb.db";
const unsigned int BLOCK_SZ = 4096;

using namespace std;
using namespace hsql;

// forward declaration headers
string operatorExpressionToString(const Expr *expr);
string expressionToString(const Expr* expression);
string exeSelect(const SelectStatement *statement);
string exeCreate(const CreateStatement *statement);
string execute(const SQLStatement *statement);

/**
 * Translates a ColumnDefinition type e.g "a INT" to a string representation
 * of the column name and data type.
 * @param column definition to convert to string
 * @return the string representation of the column definition in SQL format
 */
string columnDefToString(const ColumnDefinition* column) {
    string retString(column->name);
    switch (column->type) {
        case ColumnDefinition::DOUBLE:
            retString += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            retString += " INT";
            break;
        case ColumnDefinition::TEXT:
            retString += " TEXT";
            break;
        default:
            retString += " UNSUPPORTED TYPE";
            break;
    }
    return retString;
}

/**
 * Translates a table reference hyrise AST into it's string representation in
 * SQL format. This method starts at the AST root and makes recursive calls down
 * the left and right sides of the statement. Note that TableRef of kTableJoin
 * type have a subtype.
 * @param table : a TableRef pointer, to a portion of the statement such as
 *                foo JOIN fee ON c .. OR .. foo JOIN SELECT a, b FROM fie ON d
 * @return : The string representation of the TableRef hyrise AST in SQL form.
 */
string tableRefExprToString(const TableRef* table) {
    string retString;
    switch (table->type) {
        case kTableSelect:
            cout << "- sqlshell: Nested SELECT in JOIN DETECTED" << endl;
            // Inner Select Statement
            retString += execute(table->select);
            break;
        case kTableName:
            cout << "- sqlshell: Table name DETECTED" << endl;
            retString += table->name;
            if (table->alias != NULL) {
                retString += string(" AS ") + table->alias;
            }
            break;
        case kTableJoin:
            cout << "- sqlshell: JOIN DETECTED" << endl;
            // Traverse down the left side of the table join AST
            retString += tableRefExprToString(table->join->left);
            switch (table->join->type) {
                case kJoinInner:
                    retString += " JOIN ";
                    break;
                case kJoinLeft:
                    retString += " LEFT JOIN ";
                    break;
                default:
                    retString += " UNSUPPORTED JOIN";
                    break;

            }
            retString += tableRefExprToString(table->join->right);
            // If there is a JOIN condition
            if (table->join->condition != NULL) {
                cout << "- sqlshell: ON DETECTED" << endl;
                retString += " ON " + expressionToString(table->join->condition);
            }
            break;
        case kTableCrossProduct:
            bool notFirstExpr = false;
            for (TableRef* tableRef : *table->list) {
                if(notFirstExpr) {
                    retString += ", ";
                }
                retString += tableRefExprToString(tableRef);
                notFirstExpr = true;
            }
            break;
    }
    return retString;
}

/**
 * Utilizes the binary tree structure of an expr type (hyrise AST) to print
 * the operators and operands to console. It first addresses unary
 * operators then traverses down the left hands side of the expression
 * AST (The center most operator as the root) and then the right side.
 * note that additional logic from expressionToString is used in this
 * traversal.
 * @param opExpression : operator Expr hyrise AST
 * @return : unparsed operator expresion in SQL form as a string
 */
string operatorExprToString(const Expr* opExpression) {
    if (opExpression == NULL) {
        return "null";
    }
    string retString;

    // If expression is a unary NOT operator, add "NOT" to the
    // to the return string
    if(opExpression->opType == Expr::NOT) {
        cout << "- sqlshell: NOT DETECTED" << endl;
        retString += "NOT ";
    }

    // Call expressionToString() on the left hand side of the
    // expression linked to by member var "expr"
    retString += expressionToString(opExpression->expr) + " ";

    switch (opExpression->opType) {
        case Expr::SIMPLE_OP:
            cout << "- sqlshell: Simple Op DETECTED" << endl;
            retString += opExpression->opChar;
            break;
        case Expr::AND:
            cout << "- sqlshell: AND DETECTED" << endl;
            retString += "AND";
            break;
        case Expr::OR:
            cout << "- sqlshell: OR DETECTED" << endl;
            retString += "OR";
            break;
        default:
            break; // To avoid redundant "NOT" there is not "NOT" case
    }

    // Call expressionToString() on the right hand side of the
    // expression linked to by member var "expr2"
    if (opExpression->expr2 != NULL) {
        retString += " " + expressionToString(opExpression->expr2);
    }
    return retString;
}

/**
 * un-parses the a hyrise AST of a SQL expression and constructs its string
 * representation in SQL format. Note the call to operatorExprToString where a
 * subtree of the AST is unparsed and returned. Also note that when
 * the Expr* is of KExprColumnRef type the case will not break, allowing for the
 * addition of the left and then (in the subsequent case) the right side of
 * a table.row reference.
 *
 * @param expression : a hyrise AST of a SQL expression to be unparsed.
 * @return : the string representation of the hyrise AST in SQL form.
 */
string expressionToString(const Expr* expression) {
    string retString;
    switch (expression->type) {
        // Note this case does not break!
        case kExprColumnRef:
            cout << "- sqlshell: Column Ref DETECTED" << endl;
            if(expression->table != NULL) {
                retString += string(expression->table) + ".";
            }
        case kExprLiteralString:
            cout << "- sqlshell: String DETECTED" << endl;
            retString += expression->name;
            break;
        case kExprStar:
            cout << "- sqlshell: * DETECTED" << endl;
            retString += "*";
            break;
        case kExprLiteralInt:
            cout << "- sqlshell: Int DETECTED" << endl;
            retString += to_string(expression->ival);
            break;
        case kExprLiteralFloat:
            cout << "- sqlshell: Float DETECTED" << endl;
            retString += to_string(expression->fval);
            break;
        case kExprOperator:
            cout << "- sqlshell: Operator DETECTED" << endl;
            retString += operatorExprToString(expression);
            break;
        default:
            cout << "- sqlshell: expression not recognized!" << endl;
            retString += "EXPRESSION NOT RECOGNIZED";
            break;
    }
    return retString;
}

/**
 * Executes a select statement hyrise AST. (but for now, returns unparsed AST in
 * string SQL statement form). Note the calls to expressionToString for the
 * select statement components and the condition "WHERE" components.
 * @param statement
 * @return
 */
string exeSelect(const SelectStatement *statement) {
    cout << "- sqlshell: EXECUTING SELECT" << endl;
    string retString("SELECT ");
    bool notFirstExpr = false;
    for (Expr *expr : *statement->selectList) {
        cout << "- sqlshell: in expr loop (exSELECT)" << endl;
        if(notFirstExpr) {
            retString += ", ";
        }
        retString += expressionToString(expr);
        notFirstExpr = true;
    }
    retString += " FROM " + tableRefExprToString(statement->fromTable);
    if (statement->whereClause != NULL) {
        cout << "- sqlshell: WHERE DETECTED" << endl;
        retString += " WHERE " + expressionToString(statement->whereClause);
    }
    return retString;
}

/**
 * Executes a create statement hyrise AST. (but for now, returns unparsed AST in
 * string SQL statement form). Note the call to columnDefToString for every
 * member of the columns list (ColumnDefinition type).
 * @param statement : a hyrise AST create statement to be unparsed.
 * @return : the string representation of the create statement in SQL form.
 */
string exeCreate(const CreateStatement* statement) {
    bool notFirstExpr = false;
    string retString("CREATE ");
    retString += string(statement->tableName) + " (";
    for (ColumnDefinition* column : *statement->columns) {
        if(notFirstExpr) {
            retString += ", ";
        }
        retString += columnDefToString(column);
        notFirstExpr = true;
    }
    retString += ")";
    return retString;
}

/**
 * Executes a insert statement hyrise AST. (but for now, NOT IMPLEMENTED).
 * @param statement : a hyrise AST insert statement to be unparsed.
 * @return : the string representation of the insert statement in SQL form.
 */
string exeInsert(const InsertStatement* statement) {
    string retString("INSERT NOT FULLY SUPPORTED");
    return retString;
}

/**
 * Start of SQL hyrise AST unparsing, detects the SQL statement type and calls
 * subsequent logic specific to that statement type.
 * @param statement : the hyrise AST (SQLStatement) to be unparsed
 * @return : the SQL statement representation in a string
 */
string execute(const SQLStatement *statement) {
    string retString = "\nSQL STATEMENT:\n==============\n";

    switch (statement->type()) {
        case kStmtSelect:{
            cout << "- sqlshell: SELECT DETECTED" << endl;
            const SelectStatement* selectStmt = (const SelectStatement *) statement;
            retString += exeSelect(selectStmt);
            return retString + "\n";
        }
        case kStmtCreate:{
            const CreateStatement* createStmt = (const CreateStatement *) statement;
            retString += exeCreate(createStmt);
            return retString + "\n";
        }
        case kStmtInsert:
            return retString += "INSERT\n";
        case kStmtDrop:
            return retString += "DROP\n";
        default:
            return retString += "Not Implemented\n";
    }
}

int main(int argc, char* argv[]) {
    cout << "\n||| SQL SHELL ||| (Milestone 1) " << endl;
    if (argc <= 1) {
        fprintf(stderr, "Usage: ./sqlshell cpsc5300/data\n");
        return -1;
    }
    string envdir = string(argv[1]);
    cout << "\n in working environment: " << envdir << "\n" << endl;
    cout << "\n Enter SQL statements OR 'quit' to quit" << "\n" << endl;
    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ); // Set record length to 4K
    db.open(NULL, SQL_DB, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    string query = "";
    while(query != "quit") {
        cout << "SQL > ";
        getline(cin, query);
        cout << "\ninput query: " << query << endl;

        if (query != "quit") {
            // execute
            SQLParserResult* result = SQLParser::parseSQLString(query);
            cout << "Parse result size: " << result->size() << endl;
            if (result->isValid()) {
                for(uint i = 0; i < result->size(); i++) {
                    cout << execute(result->getStatement(i)) << endl;
                }
            } else {
                fprintf(stderr, "Given string is not a valid SQL query.\n");
                fprintf(stderr, "%s (L%d:%d)\n",
                        result->errorMsg(),
                        result->errorLine(),
                        result->errorColumn());
            }
        }
    }

    // To be used Later
    cout << "- sqlshell: END OF MAIN " << endl;
    cout << "------------------------" << endl;

    return EXIT_SUCCESS;
}
