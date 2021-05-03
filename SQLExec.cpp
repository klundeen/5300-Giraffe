#include "SQLExec.h"
#include <string.h>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

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
  // FIXME: initialize _tables table, if not yet present

  if(SQLExec::tables == nullptr){
    SQLExec::tables = new Tables();
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
  throw SQLExecError("not implemented");  // FIXME
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
  return new QueryResult("not implemented"); // FIXME
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
  return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {

  switch(statement->type){
  case ShowStatement::kTables:
    return show_tables();
  case ShowStatement::kColumns:
    return show_columns(statement);
    /*  case ShowStatement::KIndex:
        return show_index(statement);*/
  default:
    throw SQLExecError("Unrecognized SHOW type");
  }

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
       table_name != Columns::TABLE_NAME /*&&
        table_name != Indices::TABLE_NAME*/){
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

  DbRelation &table = SQLExec::tables->get_table(Columns::TABLE_NAME);

  ColumnNames *col_names = new ColumnNames;
  ColumnAttributes *col_attributes = new ColumnAttributes;
  ValueDicts *rows = new ValueDicts;
  ValueDict results;

  
  col_names->push_back("table_name");
  col_names->push_back("column_name");
  col_names->push_back("data_type");

  col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  results["table_name"] = Value(statement->tableName);
  Handles *handles = table.select(&results);
  u_long message = handles->size();

  for(auto const &handles: *handles){
    ValueDict *row = table.project(handles, col_names);
    rows->push_back(row);
  }

  delete handles;
  
  return new QueryResult(col_names, col_attributes, rows, "successfully returned " + to_string(message) + " rows");
}
  

