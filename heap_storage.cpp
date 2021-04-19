#include "heap_storage.h"
#include "storage_engine.h"
#include <map>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <cstdint>
#include "utf8.h"

using namespace std;
using std::map;

_DB_ENV = '/home/st/bruizdesomocurcio/cpsc5300/data'

int DB_BLOCK_SIZE = 4096;

RecordID SlottedPage::add(const Dbt *data) {
		if (!has_room(len(data) + 4))
		{
			throw new invalid_argument ("Not enough room in block");
		}
		num_records += 1;
		u_int16_t id = num_records;
		u_int16_t size = len(data);
		end_free -= size;
		u_int16_t loc = end_free + 1;
		put_header();
}

Dbt* SlottedPage::get(RecordID record_id) {
		u_int16_t size;
		u_int16_t loc;
		get_header(&size, &loc, record_id);
		if (loc == 0)
		{
			return NULL;
		}
		return block[loc:loc + size]; //python code
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
		size = get_n(4 * id);
		loc = get_n(4 * id + 2);
}


void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (size == ){

    }
}

u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return int.from_bytes(this.block[offset:offset + 2], this.byteorder=self.BYTE_ORDER) //python code
}



HeapFile::HeapFile(std::string name) : DbFile(name),
                                  dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
//need to add anything here? seems to be completed from header
}

HeapFile::~HeapFile(){
	this->close();
	delete this;
	//not sure if this is the right way to delete itself
}

void HeapFile::create(void){
	this->db_open(); //doesn't pass flags like python code, header doesnt accept them
	SlottedPage* block = this->get_new();
	this->put(block);
}

void HeapFile::open(void){
	this->db_open();
	// HeapFile doesn't have a block_size variable, python line not included
	//this->block_size = this->stat['re_len'];

}

void HeapFile::close(void){
		this->db.close();
		this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id){
	return SlottedPage(this->db.get(block_id), block_id);
}


void HeapFile::put(DbBlock *block){
	this->db.put(block->block_id, bytes(block->block));//not sure if bytes is a function
}

BlockIDs* HeapFile::block_ids(){
	BlockIDs* blockIDs = new BlockIDs();
	for (int i = 1; i <= this.last; i++){
		blockIDs.add(i);
	}
	return blockIDs;
}

u_int32_t HeapFile::get_last_block_id() { 
	return last; 
}


//confused about this one
void HeapFile::db_open(uint flags = 0){
	if (!this->closed){
		return;
	}
	this->db = new DbEnv();
	this->db.set_re_len(BLOCK_SZ); //does this function exist?
	this->dbfilename = _DB_ENV + this->name + '.db'; //what is this->name?
	//dbtype = DB_RECNO;// what type is this?
	this->db.open(this->dbfilename, nullptr, DB_RECNO, flags); 
	this->stat = this.db.stat(DB_FAST_STAT);//what is this->stat??
this->last = this->stat['ndata'];
	this->closed = False;
}


HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                               ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes) {
    this->file = new HeapFile(table_name);
}

void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch (DbRelationError e) {
        this->create();
    }
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            // WHERE EQUALITY CHECK
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle) {
    BlockID block_id = get<0>(handle);
    RecordID record_id = get<1>(handle);
    SlottedPage* block = this->file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = this->unmarshal(data);
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    ValueDict* row = this->project(handle);
    auto* retRow = new ValueDict();
    // STOPPED HERE!
    for (const auto& k : *column_names){
        map<Identifier, Value> map = *row;
        Value v = map[k];
        retRow->insert(pair<Identifier,Value>(k, v));
    }
    return retRow;
}

ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict localRow = *row;
    ValueDict* fullRow = new ValueDict();
    Value v;
    for (Identifier column_name : this->column_names) {
        if (localRow.find(column_name) == localRow.end()) {
            throw new DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        } else {
            v = localRow[column_name];
        }
        fullRow->insert(pair<Identifier , Value>(column_name, v));
    }
    return fullRow;
}

Handle HeapTable::append(const ValueDict *row) {
    Dbt* data = marshal(row);
    DbBlock* block = this->file.get(this->file.get_last_blockid());
    RecordID recordId;
    try {
        recordId = block->add(data);
    } catch (DbRelationError e) {
        block = this->file.get_new();
        recordId = block->add(data);
    }
    this->file.put(block);
    Handle toAppend;
    toAppend.first = block->get_block_id();
    toAppend.second = recordId;
    return toAppend;
}


void HeapTable::drop() {
    delete this->file;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u_int16_t *) (bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict* retRow = new ValueDict();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        string dataString = *(data->data);
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            string subDataString = dataString.substr(offset, offset + sizeof(int32_t)); // from offset => offset + sizeof(int32_t);
            const void* subDataPointer = &subDataString;
            cout << subDataString;
            int v;
            memcpy(&v, subDataPointer, sizeof(int32_t));
            Value* val = new Value(v);
            retRow->insert(pair<Identifier , Value>(column_name, *val));
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            string sizeDataString = dataString.substr(offset, offset + sizeof(u_int16_t)); // Make Constant for 2
            const void* sizeDataPointer = &sizeDataString;
            int size;
            memcpy(&size, sizeDataPointer, sizeof(u_int16_t));
            offset += 2;
            string subDataString = dataString.substr(offset, offset + size);
            const void* subDataPointer = &subDataString;
            cout << subDataString;
            char* v;
            memcpy(&v, subDataPointer, size);
            offset += size;
            Value* val = new Value(v);
            retRow->insert(pair<Identifier , Value>(column_name, *val));
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    return retRow;
}

bool test_heap_storage() {return true;}


