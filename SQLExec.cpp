#include "SQLExec.h"
#include <string.h>

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

QueryResult::~QueryResult() {

  if(column_names != nullptr){
    delete column_names;
  }

  if (column_attributes != nullptr){
    delete column_attributes;
  }

  if (rows != nullptr){
    for (auto row: *rows){
      delete row;
    }
    delete rows;
  }

}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
  
  if(SQLExec::tables == nullptr){
    SQLExec::tables = new Tables();
  }

  if(SQLExec::indices == nullptr){
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
    default:
      return new QueryResult("not implemented");
    }
  } catch (DbRelationError &e) {
    throw SQLExecError(string("DbRelationError: ") + e.what());
  }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
  column_name = col->name;
  switch (col->type) {
  case ColumnDefinition::INT:
    column_attribute = ColumnAttribute(ColumnAttribute::INT);
    break;
  case ColumnDefinition::TEXT:
    column_attribute = ColumnAttribute(ColumnAttribute::TEXT);
    break;
  default:
    throw SQLExecError("unsupported data type");
  }
}


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

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    if(statement->type != CreateStatement::kIndex)
        throw SQLExecError("unrecognized CREATE type");

    // get the index name and type
    Identifier indexName = statement->indexName;
    Identifier indexType = statement->indexType;

    // get the table name
    Identifier tableName = statement->tableName;

    // get column names of table
    ColumnNames colNames;
    ColumnAttributes colAttributes;
    ValueDict where;
    where["table_name"] = tableName;
    ValueDict results;

    DbRelation &columnsTable = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *colHandles = columnsTable.select(&where);
    for (auto const &handle: *colHandles) {
        results = *columnsTable.project(handle);
        colNames.push_back(results["column_name"].s);
    }
    delete colHandles;

    // get index column names and check if they exist in the table columns
    char* indexCol;
    std::vector<char*> indexCols;
    for(char* col: *statement->indexColumns) {
        indexCol = col;
        if(std::find(colNames.begin(), colNames.end(), indexCol) == colNames.end())
            throw SQLExecError("index column not found in table");
        indexCols.push_back(indexCol);
    }

    // if all index columns exist in the table, add them to the _indices table
    DbRelation &indexRelationTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handle indexColHandle;
    Handles indexColumnHandles;
    int colNum = 0;
    ValueDict row;
    row["table_name"] = tableName;
    row["index_name"] = indexName;
    row["index_type"] = Value(indexType);
    for(char* col: indexCols) {
        row["column_name"] = Value(*col);
        row["seq_in_index"] = ++colNum;
        if(indexType == "BTREE")
            row["is_unique"] = true;
        else
            row["is_unique"] = false;
        indexColHandle = indexRelationTable.insert(&row);
        indexColumnHandles.push_back(indexColHandle);
    }

    // DbIndex &newIndex = SQLExec::indices->get_index(tableName, indexName);
    // newIndex.create();

    return new QueryResult("created " + indexName);
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
  if(statement->type != CreateStatement::kTable)
    throw SQLExecError("unrecognized CREATE type");
  
  // get the table name
  Identifier tableName = statement->tableName;
  
  // get column names and attributes
  Identifier colName;
  ColumnNames colNames;
  ColumnAttribute colAttribute;
  ColumnAttributes colAttributes;
  for(ColumnDefinition* col: *statement->columns) {
    column_definition(col, colName, colAttribute);
    colNames.push_back(colName);
    colAttributes.push_back(colAttribute);
  }
  
  // where statement
  ValueDict where;
  where["table_name"] = Value(tableName);
  
  // add to _tables relation
  Handle tablesHandle = SQLExec::tables->insert(&where);
  
  // add to _columns relation
  try {
    Handles columnsTableHandles;
    DbRelation &columnsTable = SQLExec::tables->get_table(Columns::TABLE_NAME);
    try {
      for(uint16_t i = 0; i < colNames.size(); i++) {
        where["column_name"] = Value(colNames[i]);
        where["data_type"] = Value(colAttributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
        columnsTableHandles.push_back(columnsTable.insert(&where));
      }
      
      DbRelation &table = SQLExec::tables->get_table(tableName);
      if (statement->ifNotExists)
        table.create_if_not_exists();
      else   
        table.create();
    } catch (exception &e) {
      // attempt to remove from _columns
      try {
        for (auto const &handle: columnsTableHandles)
          columnsTable.del(handle);
      } catch (...) {}
      throw;
    }
  } catch (exception &e) {
    try {
      // attempt to remove from _tables
      SQLExec::tables->del(tablesHandle);
    } catch (...) {}
    throw;
  }
  
  return new QueryResult("created " + tableName);
}

// DROP ...
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


// DROP ...
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
  // check that statement is a drop TABLE statement
  if(statement->type != DropStatement::kTable) {
    throw SQLExecError("unrecognized DROP type");
  }
  
  // get the table
  Identifier tableName = statement->name; 
  if(tableName == Tables::TABLE_NAME || tableName == Columns::TABLE_NAME)
    throw SQLExecError("cannot drop schema tables");
  
  DbRelation &table = SQLExec::tables->get_table(tableName);
  // FIXME - test whether table is null or other way to indicate that table doesn't exist?
  
  // where statement
  ValueDict where;
  where["table_name"] = Value(tableName);
  
  // remove from _columns schema
  DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
  Handles *handles = columns.select(&where);
  for (auto const &handle: *handles) {
    columns.del(handle);
  }
  delete handles;
  
  // remove table (drop and remove from cache, which I think is automatic when calling drop())
  table.drop();
  
  // remove table from _tables schema
  SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); 
  delete handles;//added in, wasnt there
  
  return new QueryResult(std::string("dropped " + tableName));
}

QueryResult *SQLExec::drop_index(const DropStatement *statement){

  //check if the statement is a drop statement
  if(statement->type != DropStatement::kTable){
    throw SQLExecError("unrecognized DROP type");
  }
  
  Identifier table_name = statement->name;
  Identifier index_name = statement->indexName;
  ValueDict where;

  where["table_name"] = Value(table_name);
  where["index_name"] = Value(index_name);


  DbIndex &index = SQLExec::indices->get_index(table_name, index_name);

  //remove index
  index.drop();

  //remove rows from _indices
  Handles *handles = SQLExec::indices->select(&where);
  for(auto const &handle: *handles){
    SQLExec::indices->del(handle);
  }
  delete handles;

  return new QueryResult(std::string("dropped " + index_name));
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
  
  switch(statement->type){
  case ShowStatement::kTables:
    return show_tables();
  case ShowStatement::kColumns:
    return show_columns(statement);
  case ShowStatement::kIndex:
    return show_index(statement);
  default:
    throw SQLExecError("Unrecognized SHOW type");
  }
  
}

QueryResult *SQLExec::show_index(const ShowStatement *statement){

  Identifier table_name = statement->tableName;
  ColumnNames *col_names = new ColumnNames;
  ColumnAttributes *col_attributes = new ColumnAttributes;

  col_names->push_back("table_name");
  col_names->push_back("index_name");
  col_names->push_back("seq_in_index");
  col_names->push_back("column_name");
  col_names->push_back("index_type");
  col_names->push_back("is_unique");
  col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
  

  ValueDict where;
  where["table_name"] = Value(table_name);

  Handles *handles = SQLExec::indices->select(&where);
  u_long message = handles->size();


  ValueDicts *rows = new ValueDicts;
  for (auto const &handle: *handles){
    ValueDict *row = SQLExec::indices->project(handle, col_names);
    Identifier table_name = row->at("table_name").s;
    if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME){
      rows->push_back(row);
    }
    else{
      delete row;
    }
  }
  delete handles;
  
  
  return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(message) + " rows");
}


QueryResult *SQLExec::show_tables() {
  
  ColumnNames *col_names = new ColumnNames;
  ColumnAttributes *col_attributes = new ColumnAttributes;
  ValueDicts *rows = new ValueDicts;
  
  col_names->push_back("table_name");
  col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
  
  Handles *handles = SQLExec::tables->select();
  u_long message = handles->size() - 2;

  for(auto const &handles: *handles){
    ValueDict *row = SQLExec::tables->project(handles, col_names);
    Identifier table_name = row->at("table_name").s;
    if(table_name != Tables::TABLE_NAME &&
       table_name != Columns::TABLE_NAME &&
       table_name != Indices::TABLE_NAME){
      rows->push_back(row);
    }
    else{
      delete row;
    }
  }

  delete handles;

  return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(message) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {

  Identifier table_name = statement->tableName;
  DbRelation &table = SQLExec::tables->get_table(Columns::TABLE_NAME);

  ColumnNames *col_names = new ColumnNames;
  ColumnAttributes *col_attributes = new ColumnAttributes;
  ValueDicts *rows = new ValueDicts;
  ValueDict results;

  
  col_names->push_back("table_name");
  col_names->push_back("column_name");
  col_names->push_back("data_type");

  col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  results["table_name"] = Value(table_name);
  Handles *handles = table.select(&results);
  u_long message = handles->size();

  for(auto const &handles: *handles){
    ValueDict *row = table.project(handles, col_names);
    rows->push_back(row);
  }

  delete handles;
  
  return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(message) + " rows");
}
  

