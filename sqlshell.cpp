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

// forward declaration
string operatorExpressionToString(const Expr *expr);

string operatorExprToString(const Expr* opExpresion) {
    if (opExpresion == NULL) {
        return "null";
    }

    return "TODO";
}

string expressionToString(const Expr *expression) {
    string retString;
    switch (expression->type) {
        case kExprStar:
            cout << "* DETECTED" << endl;
            retString += "*";
            break;
        case kExprLiteralInt:
            retString += to_string(expression->ival);
            break;
        case kExprLiteralFloat:
            retString += to_string(expression->fval);
            break;
        case kExprLiteralString:
            retString += expression->name;
            break;
        case kExprOperator:
            retString += operatorExprToString(expression);
            break;
        default:
            retString += "EXPRESSION NOT RECOGNIZED";
            break;
    }
    return retString;
}


string exeSelect(const SelectStatement *statement) {
    string retString("SELECT");
    bool notFirstExpr = false;
    cout << "IN exe SELECT" << endl;
    for (Expr *expr : *statement->selectList) {
        cout << "expr loop from select list" << endl;
        if(notFirstExpr) {
            retString += ", ";
        }
        retString += expressionToString(expr);
        notFirstExpr = true;
    }
    //ret += " FROM " + tableRefInfoToString(statement->fromTable);
    return retString;
}

string exeCreate(const CreateStatement *statement) {
    return "TODO";
}

string execute(const SQLStatement *statement) {
    string retString;
    switch (statement->type()) {
        case kStmtSelect:{
            cout << "SELECT DETECTED" << endl;
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
