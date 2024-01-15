#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SqlExecutor.h"

using namespace std;
using namespace hsql;

const unsigned int BLOCK_SZ = 4096;

/**
 * Initializes the database environment.
 * @param envdir Directory path for the database environment.
 */
void initializeDbEnv(string envdir)
{
    // setup the database environment
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    env.open(envdir.c_str(), DB_CREATE, 0);

    printf("(sql5300: running with database environment at %s)\n", envdir.c_str());
}

int main(int argc, char *argv[])
{
    // Check if the number of arguments is at least 2 
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <envdir>" << std::endl;
        return 1; 
    }

    string envdir = argv[1];

    initializeDbEnv(envdir);

    string userInput;
    while (true)
    {
        cout << "SQL> ";
        getline(cin, userInput);
        if (userInput == "quit")
        {
            break;
        }

        SQLParserResult *result = SQLParser::parseSQLString(userInput);

        if (result->isValid())
        {
            SqlExecutor executor;
            string parsedStatement = executor.execute(result->getStatement(0));
            cout << parsedStatement << endl;
        }
        else
        {
            cout << "Invalid SQL: " << userInput << endl;
        }
        delete result;
    }

    return EXIT_SUCCESS;
}
