/**
 * @file sql5300.cpp
 *
 * This program is the main application for Milestone 1: Skeleton in Sprint: Verano
 * The code sets up an environment and runs a SQL shell emulator.
 * Users can write SQL commands which are then parsed and "executed".
 * The program runs interactively until the user issues a quit command.
 * This code use Berkeley DB and sql-parser libraries
 * Author: Noha Nomier,  CPSC5300 WQ2024
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SqlExecutor.h"
#include "heap_storage.h"

using namespace std;
using namespace hsql;

/*
 * we allocate and initialize the _DB_ENV global
 */
DbEnv *_DB_ENV;

int main(int argc, char *argv[])
{
    // Open/create the db enviroment
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;

    string userInput;
    while (true)
    {
        cout << "SQL> ";
        getline(cin, userInput);
        if (userInput.length() == 0)
            continue; // blank line -- just skip
        if (userInput == "quit")
        {
            break;
        }

        if (userInput == "test")
        {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        } else if (userInput == "slotted") {
            cout << "test_slotted_page: " << (test_slotted_page() ? "ok" : "failed") << endl;
            continue;
        } else if (userInput == "heapfile") {
            cout << "test_heap_file: " << (test_heap_file() ? "ok" : "failed") << endl;
            continue;
        }

        SQLParserResult *result = SQLParser::parseSQLString(userInput);
        if (!result->isValid())
        {
            cout << "invalid SQL: " << userInput << endl;
            delete result;
            continue;
        }
        SqlExecutor executor;
        // execute the statement
        for (uint i = 0; i < result->size(); ++i)
        {
            cout << executor.execute(result->getStatement(i)) << endl;
        }
        delete result;
    }
    return EXIT_SUCCESS;
}
