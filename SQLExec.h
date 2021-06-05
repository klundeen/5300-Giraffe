/**
 * @file SQLExec.h - SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#pragma once

#include <exception>
#include <string>
#include "SQLParser.h"
#include "schema_tables.h"

/**
 * @class SQLExecError - exception for SQLExec methods
 */
class SQLExecError : public std::runtime_error {
public:
    explicit SQLExecError(std::string s) : runtime_error(s) {}
};


/**
 * @class QueryResult - data structure to hold all the returned data for a query execution
 */
class QueryResult {
public:
    QueryResult() : column_names(nullptr), column_attributes(nullptr), rows(nullptr), message("") {}

    QueryResult(std::string message) : column_names(nullptr), column_attributes(nullptr), rows(nullptr),
                                       message(message) {}

    QueryResult(ColumnNames *column_names, ColumnAttributes *column_attributes, ValueDicts *rows, std::string message)
            : column_names(column_names), column_attributes(column_attributes), rows(rows), message(message) {}

    virtual ~QueryResult();

    ColumnNames *get_column_names() const { return column_names; }

    ColumnAttributes *get_column_attributes() const { return column_attributes; }

    ValueDicts *get_rows() const { return rows; }

    const std::string &get_message() const { return message; }

    friend std::ostream &operator<<(std::ostream &stream, const QueryResult &qres);

protected:
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDicts *rows;
    std::string message;
};


/**
 * @class SQLExec - execution engine
 */
class SQLExec {
public:
    /**
     * Execute the given SQL statement.
     * @param statement   the Hyrise AST of the SQL statement to execute
     * @returns           the query result (freed by caller)
     */
    static QueryResult *execute(const hsql::SQLStatement *statement);

protected:
    // the one place in the system that holds the _tables table and _indices table
    static Tables *tables;
    static Indices *indices;

    // recursive decent into the AST
    /**
     * Creates the specified table with provided columns in statement
     * @param statement     statement with table to create with specified columns
     * @return QueryResult  The result of the create query
     */
    static QueryResult *create(const hsql::CreateStatement *statement);

    /**
     * Creates the specified table with provided columns in statement
     * @param statement     statement with table to create with specified columns
     * @return QueryResult  The result of the create query
     */
    static QueryResult *create_table(const hsql::CreateStatement *statement);
    
    /**
     * Creates the specified index for the provided table name in statement
     * @param statement     statement with index to create with specified table
     * @return QueryResult  The result of the create query
     */
    static QueryResult *create_index(const hsql::CreateStatement *statement);

    /**
     * Drop the specified table name or index
     * @param statement     statement with table name/index to drop
     * @return QueryResult  The result of the drop query
     */
    static QueryResult *drop(const hsql::DropStatement *statement);

    /**
     * Drop the specified table name or index
     * @param statement     statement with table name to drop
     * @return QueryResult  The result of the drop query
     */
    static QueryResult *drop_table(const hsql::DropStatement *statement);

    /**
     * Drop the specified index
     * @param statement     statement with index to drop
     * @return QueryResult  The result of the drop query
     */
    static QueryResult *drop_index(const hsql::DropStatement *statement);

    /**
     * Internal method for show statement, shall call show columns, table, index...
     * @return QueryResult  The result of the show query
     */
    static QueryResult *show(const hsql::ShowStatement *statement);

    /**
     * Shows all current tables in DB
     * @return QueryResult  The result of the show columns query
     */
    static QueryResult *show_tables();

    /**
     * Shows all columns for the provided table name
     * @param statement     statement with table name
     * @return QueryResult  The result of the show columns query
     */
    static QueryResult *show_columns(const hsql::ShowStatement *statement);

    /**
     * Display all the availalbe indexes for the provided table name
     * @param statement     state with table name to get index
     * @return QueryResult  The result of the show index query
     */
    static QueryResult *show_index(const hsql::ShowStatement *statement);

    /**
     * Inserts the data into the table. Will return exception if any
     *  invalid user input (incorrect table, incorrect columns,...)
     * @param statement  InsertStatement parsed from user input
     * @return           QueryResult
     */
    static QueryResult *insert(const hsql::InsertStatement *statement);

    /**
     * Deletes the data from the table. Will return exception if any
     *  invalid user input (incorrect table)
     * @param statement  DeleteStatement parsed from user input
     * @return           QueryResult
     */
    static QueryResult *del(const hsql::DeleteStatement *statement);

    /**
     * Returns the data from the table. Will return exception if any
     *  invalid user input (incorrect table, columns)
     * @param statement  SelectStatement parsed from user input
     * @return           QueryResult
     */
    static QueryResult *select(const hsql::SelectStatement *statement);

    /**
     * check if the provided table exists in metadata
     * @param tableName  table name to check
     * @returns          bool indicating if the provided table name exists
     */
    static bool checkIfTableExists(const char* tableName);

    /**
     * check if the provided column names exist for the provided table
     * @param columns    vector/list of char pointers that would be obtained as indexColumns from CreateStatement
     * @param indexname  table name to check for
     */
    static void checkIfColumnsExists(const std::vector<char*>* columns, const char* tableName);
    
    /**
     * check if the provided index is created for the provided table
     * @param tableName  table name to check for
     * @param indexname  index name to check
     * @returns          bool indicating if the provided index exists for table
     */
    static bool checkIfIndexExists(const char* tableName, const char* indexname);

    /**
     * Pull out column name and attributes from AST's column definition clause
     * @param col                AST column definition
     * @param column_name        returned by reference
     * @param column_attributes  returned by reference
     */
    static void column_definition(const hsql::ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute);

    /**
     * Returns the a map of column and value matches look for. Only allows
     *  'AND' operator and '=' comparison, will throw exception otherwise.
     *  invalid user input (incorrect table, columns)
     * @param expr          where statement expression parsed from 
     *                          select/delete statement
     * @param column_names  All the column names of the table
     * @return              map of column and values to match for.
     */
    static ValueDict* get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names);
};

