#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

const char *HOME = "cpsc5300/data";
const char *EXAMPLE = "example.db";
const unsigned int BLOCK_SZ = 4096;

const char *home = getenv("HOME");
string envdir = string(home) + "/" + HOME;

void initializeDbEnv()
{
    // setup the database environment
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    printf("(sql5300: running with database environment at %s)\n", envdir.c_str());
}

int main(void)
{
    initializeDbEnv();

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
            cout << "Valid sql: " << userInput << endl;
        }
        else
        {
            cout << "Invalid sql: " << userInput << endl;
        }
        delete result;
    }

    return EXIT_SUCCESS;
}