/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"
#include "EvalPlan.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/**
 * Cleanup method for the QueryResult
 */
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

/**
 * Execute the given SQL statement.
 * @param statement   the Hyrise AST of the SQL statement to execute
 * @returns           the query result (freed by caller)
 */
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
 * Inserts the data into the table. Will return exception if any
 *  invalid user input (incorrect table, incorrect columns,...)
 * @param statement  InsertStatement parsed from user input
 * @return           QueryResult
 */
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    // check if table exists
    if (!checkIfTableExists(statement->tableName)) {
        throw DbRelationError("table '" + string(statement->tableName) + "' does not exist");
    }

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(statement->tableName);
    
    const ColumnNames all_column_names = table.get_column_names();
    ColumnNames *column_names = new ColumnNames;
    // ColumnAttributes *column_attributes = new ColumnAttributes;

    if (statement->columns != nullptr) {
        if (statement->columns->size() > all_column_names.size()) {
            throw DbRelationError("provided columns in insert statement do not match from expected");
        } else if (statement->columns->size() < all_column_names.size()) {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        }
        
        for (auto const col : *statement->columns) {
            if (find(all_column_names.begin(), all_column_names.end(), col) == all_column_names.end()) {
                throw SQLExecError("Invalid column name '" + string(col)+ "'");
            }
        }

        for (auto const col : *statement->columns){
            column_names->push_back(col);
        }
    } else {
        for (auto const col : all_column_names){
            column_names->push_back(col);
        }
    }

    ValueDict row;
    for (uint i =0; i < statement->values->size(); i++){
        if (statement->values->at(i)->type == hsql::kExprLiteralString) {
            row[column_names->at(i)] = Value(statement->values->at(i)->name);
        } else if (statement->values->at(i)->type == hsql::kExprLiteralInt) {
            row[column_names->at(i)] = Value(statement->values->at(i)->ival);
        } else {
            throw SQLExecError("Insert type is not implemented");
        }
    }

    // insert row into table
    Handle handle = table.insert(&row);

    // Get indexes and insert the values
    IndexNames indexes = SQLExec::indices->get_index_names(statement->tableName);
    for(Identifier name : indexes){
        DbIndex& index = SQLExec::indices->get_index(statement->tableName, name);
        index.insert(handle);
    }
    
    return new QueryResult("Successfully inserted 1 row into " + string(statement->tableName) + " and " + to_string(indexes.size()) + " indices");
}

/**
 * Deletes the data from the table. Will return exception if any
 *  invalid user input (incorrect table)
 * @param statement  DeleteStatement parsed from user input
 * @return           QueryResult
 */
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    // check if table exists
    if (!checkIfTableExists(statement->tableName)) {
        throw DbRelationError("table '" + string(statement->tableName) + "' does not exist");
    }

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(statement->tableName);
    
    // get all columns to for get where conjunction
    const ColumnNames all_column_names = table.get_column_names();


    EvalPlan *deletePlan = new EvalPlan(table);

    // build where object from the expression provided
    ValueDict *wherePlanInput = get_where_conjunction(statement->expr, &all_column_names);
    if (wherePlanInput != nullptr) {
        deletePlan = new EvalPlan(wherePlanInput, deletePlan);
    }
    EvalPipeline pipeline = deletePlan->optimize()->pipeline();

    // get indexes and for the table
    IndexNames indexes = SQLExec::indices->get_index_names(statement->tableName);
    Handles *handles = pipeline.second;

    //now delete all the handles
    for( auto const &handle: *handles) {
        for (uint i = 0; i < indexes.size(); i++) {
            DbIndex &index = SQLExec::indices->get_index(statement->tableName, indexes.at(i));
            index.del(handle);
        }
    }
    uint numHandles = handles->size();
    //remove from table
    for (auto const& handle: *handles){
        table.del(handle);
    }
    
    return new QueryResult("successfully deleted " + to_string(numHandles)+ " rows from " + string(statement->tableName) + " and " + to_string(indexes.size()) + " indices");
}

/**
 * Returns the data from the table. Will return exception if any
 *  invalid user input (incorrect table, columns)
 * @param statement  SelectStatement parsed from user input
 * @return           QueryResult
 */
QueryResult *SQLExec::select(const SelectStatement *statement) {
    // check if table exists
    if (!checkIfTableExists(statement->fromTable->name)) {
        throw DbRelationError("table '" + string(statement->fromTable->name) + "' does not exist");
    }

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(statement->fromTable->name);
    
    ColumnNames *column_names = new ColumnNames;
    ColumnNames all_column_names = table.get_column_names();
    ColumnAttributes *column_attributes = new ColumnAttributes;

    EvalPlan *selectPlan = new EvalPlan(table);

    // build conjunction from all the columns, not just the project ones.
    ValueDict *wherePlanInput = get_where_conjunction(statement->whereClause, &all_column_names);
    if (wherePlanInput != nullptr) {
        selectPlan = new EvalPlan(wherePlanInput, selectPlan);
    }
    // check if the projected columns are all or selective
    if (statement->selectList->size() > 0 && (statement->selectList->at(0)->type == hsql::kExprStar)) {
        for (const auto col: all_column_names){
            column_names->push_back(col);
        }
    } else {
        for (hsql::Expr* col: *statement->selectList) {
            column_names->push_back(Identifier(col->name));
        }
        column_attributes = table.get_column_attributes(*column_names);
    }
    selectPlan = new EvalPlan(column_names, selectPlan);
    
    //optimize the plan and evaluate the optimized plan
    ValueDicts *rows = selectPlan->optimize()->evaluate();
    return new QueryResult(column_names, column_attributes, rows,
                           "successfuly returned " + to_string(rows->size()) + " rows");
}

/**
 * Pull out column name and attributes from AST's column definition clause
 * @param col                AST column definition
 * @param column_name        returned by reference
 * @param column_attributes  returned by reference
 */
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

/**
 * Creates the specified table with provided columns in statement
 * @param statement     statement with table to create with specified columns
 * @return QueryResult  The result of the create query
 */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
 * Creates the specified table with provided columns in statement
 * @param statement     statement with table to create with specified columns
 * @return QueryResult  The result of the create query
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

/**
 * Creates the specified index for the provided table name in statement
 * @param statement     statement with index to create with specified table
 * @return QueryResult  The result of the create query
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        try {
            index.create();
        } catch (exception& e) {
            index.drop();
            throw;  // re-throw the original exception (which should give the client some clue as to why it did
            
        }

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

/**
 * Drop the specified table name or index
 * @param statement     statement with table name/index to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

/**
 * Drop the specified table name or index
 * @param statement     statement with table name to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

/**
 * Drop the specified index
 * @param statement     statement with index to drop
 * @return QueryResult  The result of the drop query
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

/**
 * Internal method for show statement, shall call show columns, table, index...
 * @return QueryResult  The result of the show query
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

/**
 * Display all the availalbe indexes for the provided table name
 * @param statement     state with table name to get index
 * @return QueryResult  The result of the show index query
 */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

/**
 * Shows all current tables in DB
 * @return QueryResult  The result of the show columns query
 */
QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

/**
 * Shows all columns for the provided table name
 * @param statement     statement with table name
 * @return QueryResult  The result of the show columns query
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

/**
 * check if the provided table exists in metadata
 * @param tableName  table name to check
 * @returns          bool indicating if the provided table name exists
 */
bool SQLExec::checkIfTableExists(const char* tableName) {
    // setup query to see if table exists
    ValueDict where;
    where["table_name"] = Value(tableName);
    Handles *handles = SQLExec::tables->select(&where);
    bool resp = handles->size() > 0;
    
    delete handles;

    return resp;
}

/**
 * check if the provided column names exist for the provided table
 * @param columns    vector/list of char pointers that would be obtained as indexColumns from CreateStatement
 * @param indexname  table name to check for
 */
void SQLExec::checkIfColumnsExists(const std::vector<char*>* columns, const char* tableName) {
    // get column table pointer
    DbRelation &columnTable = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // setup column names for the table metdata
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    // get all the colums for the provided table
    ValueDict where;
    where["table_name"] = Value(tableName);
    Handles *handles = columnTable.select(&where);
    std::vector<Value> *tableColumnsFound = new std::vector<Value>();
    for (auto const &handle: *handles) {
        ValueDict *row = columnTable.project(handle, column_names);
        tableColumnsFound->push_back(row->at("column_name"));
    }

    // check if those columns exist and return exception if not
    for (auto const &col: *columns) {
        bool found = false;
        string strCol = string(col);
        for(const Value &tableCol: *tableColumnsFound) {
            if (strCol.compare(tableCol.s) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw SQLExecError("column '" + strCol + "' does not exist for table '" + string(tableName)+ "'");
        }
    }

    delete handles;
}

/**
 * check if the provided index is created for the provided table
 * @param tableName  table name to check for
 * @param indexname  index name to check
 * @returns          bool indicating if the provided index exists for table
 */
bool SQLExec::checkIfIndexExists(const char* tableName, const char* indexName) {
    // get pointer to the index table
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    
    // get all the indices for the provided table and index name
    ValueDict where;
    where["table_name"] = Value(tableName);
    where["index_name"] = Value(indexName);
    Handles *duplicateCheck = indexTable.select(&where);
    // check and return if more than one, delete any empty pointers
    bool resp = duplicateCheck->size() > 0;
    delete duplicateCheck;
    return resp;
}

/**
 * Returns the a map of column and value matches look for. Only allows
 *  'AND' operator and '=' comparison, will throw exception otherwise.
 *  invalid user input (incorrect table, columns)
 * @param expr          where statement expression parsed from 
 *                          select/delete statement
 * @param column_names  All the column names of the table
 * @return              map of column and values to match for.
 */
ValueDict *SQLExec::get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names) {
    // check if there is no where statement, if so just return a null
    if (expr == nullptr) {
        return nullptr;
    }

    // throw error if unsupported operator (conjunction type) is passed 
    if(expr->type != hsql::kExprOperator) {
        throw DbRelationError("Unsupported operator passed!");
    }
    ValueDict* rows = new ValueDict;
    if (expr->opType == Expr::AND) {
        ValueDict* sub = get_where_conjunction(expr->expr, col_names);
        if (sub != nullptr){
            rows->insert(sub->begin(), sub->end());
        }
        sub = get_where_conjunction(expr->expr2, col_names);
        rows->insert(sub->begin(), sub->end());
    } else if (expr->opType == Expr::SIMPLE_OP) {
        if(expr->opChar != '=') {
            throw DbRelationError("only equality predicates currently supported");
        }
        Identifier col = expr->expr->name;
        if(find(col_names->begin(), col_names->end(), col) == col_names->end()){
            throw DbRelationError("unknown column '" + col + "' in where statement");
        }
        if(expr->expr2->type == hsql::kExprLiteralString) {
            rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->name)));
        }
        else if(expr->expr2->type == hsql::kExprLiteralInt) {
            rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->ival)));
        }
        else {
            throw DbRelationError("Value in where comparison is not supported");
        }
    } else {
        throw DbRelationError("only supports AND conjunctions");
    }

    return rows;
}
