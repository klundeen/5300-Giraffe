#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
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

string columnDefToString(const ColumnDefinition* col) {
    string retString(col->name);
    switch (col->type) {
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

string tableRefExprToString(const TableRef* table) {
    string retString;
    switch (table->type) {
        case kTableSelect:
            cout << "- sqlshell: Nested SELECT in JOIN DETECTED" << endl;
            // Inner Select Statement
            retString += execute(table->select);
            break;
        case kTableName:
            retString += table->name;
            if (table->alias != NULL) {
                retString += string(" AS ") + table->alias;
            }
            break;
        case kTableJoin:
            // Traverse down the left side of the table join AST
            retString += tableRefExprToString(table->join->left);
            switch (table->join->type) {
                case kJoinInner:
                    retString += " JOIN ";
                    break;
                case kJoinLeft:
                    retString += " LEFT JOIN ";
                    break;
                    /*
                    case kJoinCross:
                        retString += " UNSUPPORTED JOIN";
                        break;
                    case kJoinOuter:
                        retString += " UNSUPPORTED JOIN";
                        break;
                    case kJoinLeftOuter:
                        retString += " UNSUPPORTED JOIN";
                        break;
                    case kJoinRightOuter:
                        retString += " UNSUPPORTED JOIN";
                        break;
                    */
                default:
                    retString += " UNSUPPORTED JOIN";
                    break;

            }
            retString += tableRefExprToString(table->join->right);
            // If there is a JOIN condition
            if (table->join->condition != NULL) {
                retString += " ON " + expressionToString(table->join->condition);
            }
            break;
        default:
            retString += "UNSUPPORTED JOIN";
            break;
    }
    return retString;
}

/*
 * Utilizes the binary tree structure of an expr type to print
 * the operators and operands to console. It first addresses unary
 * operators then traverses down the left hands side of the expression
 * AST (The center most operator as the root) and then the right side.
 * note that additional logic from expressionToString is used in this
 * traversal.
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

    // Call expressionToString() on the left hand of the
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

    if (opExpression->expr2 != NULL) {
        retString += " " + expressionToString(opExpression->expr2);
    }
    return retString;
}

string expressionToString(const Expr *expression) {
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


string exeSelect(const SelectStatement *statement) {
    string retString("SELECT ");
    bool notFirstExpr = false;
    cout << "- sqlshell: in SELECT exe" << endl;
    for (Expr *expr : *statement->selectList) {
        cout << "- sqlshell: in expr loop" << endl;
        if(notFirstExpr) {
            retString += ", ";
        }
        retString += expressionToString(expr);
        notFirstExpr = true;
    }
    retString += " FROM " + tableRefExprToString(statement->fromTable);
    if (statement->whereClause != NULL) {
        retString += " WHERE " + expressionToString(statement->whereClause);
    }
    return retString;
}

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

string exeInsert(const InsertStatement* statement) {
    string retString("INSERT NOT FULLY SUPPORTED");
    return retString;
}

string execute(const SQLStatement *statement) {
    string retString;
    switch (statement->type()) {
        case kStmtSelect:{
            cout << "- sqlshell: SELECT DETECTED" << endl;
            const SelectStatement* selectStmt = (const SelectStatement *) statement;
            retString = exeSelect(selectStmt);
            return retString;
        }
        case kStmtCreate:{
            const CreateStatement* createStmt = (const CreateStatement *) statement;
            retString = exeCreate(createStmt);
            return retString;
        }
        case kStmtInsert:
            return "INSERT";
        case kStmtDrop:
            return "DROP";
        default:
            return "Not Implemented";
    }
}

int main(int argc, char* argv[]) {
    if (argc <= 1) {
        fprintf(stderr, "Usage: ./sqlshell cpsc5300/data\n");
        return -1;
    }
    string envdir = string(argv[1]);
    cout << "\n envdir: " << envdir << endl;

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
        cout << query << endl;

        // execute
        SQLParserResult* result = SQLParser::parseSQLString(query);
        cout << "Parse result size: " << result->size() << endl;
        if (result->isValid()) {
            for(uint i = 0; i < result->size(); i++) {
                cout << execute(result->getStatement(i)) << endl;
                cout << "HERE!" << endl;
            }
            cout << "HERE! 2" << endl;
        } else {
            fprintf(stderr, "Given string is not a valid SQL query.\n");
            fprintf(stderr, "%s (L%d:%d)\n",
                    result->errorMsg(),
                    result->errorLine(),
                    result->errorColumn());
        }
    }

    char block[BLOCK_SZ];
    Dbt data(block, sizeof(block));
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = 1;
    strcpy(block, "hello!");
    db.put(NULL, &key, &data, 0);  // write block #1 to the database

    Dbt rdata;
    db.get(NULL, &key, &rdata, 0); // read block #1 from the database
    cout << "Read (block #" << block_number << "): '" << (char *)rdata.get_data() << "'";
    cout << " (expect 'hello!')" << std::endl;

    return EXIT_SUCCESS;
}
