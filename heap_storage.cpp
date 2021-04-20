#include "heap_storage.h"
#include "storage_engine.h"
#include <map>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <cstdint>

using namespace std;
using std::map;

string DIR = "~/cpsc5300/data/";

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u_int16_t id = ++this->num_records;
    u_int16_t size = (u_int16_t) data->get_size();
    this->end_free -= size;
    u_int16_t loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
		u_int16_t size;
		u_int16_t loc;
		get_header(size, loc, record_id);
		if (loc == 0)
		{
			return nullptr;
		}
		Dbt* data = new Dbt();
        memcpy(data->get_data(), address(loc), size);
		return data;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u_int16_t size;
    u_int16_t loc;
    this->get_header(size,loc, record_id);
    u_int32_t newSize = data.get_size();
    if (newSize > size) {
        u_int16_t extra = newSize - size;
        if (!has_room(extra)) {
            throw new DbRelationError("Not enough room in block");
        }
        slide(loc + newSize, loc + size);
        memcpy(address(loc - extra), data.get_data(), newSize);
    } else {
        memcpy(address(loc), data.get_data(), newSize);
        slide(loc + newSize, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, newSize, loc);
}

void SlottedPage::del(RecordID record_id) {
    u_int16_t size;
    u_int16_t loc;
    get_header(size, loc, record_id);
    put_header(record_id);
    slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs* records = new RecordIDs();
    for (int i = 1; i < num_records + 1; i++) {
        u_int16_t size;
        u_int16_t loc;
        get_header(size, loc, i);
        if (size != 0) {
            records->push_back((RecordID)i);
        }
    }
    return records;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
		size = get_n(4 * id);
		loc = get_n(4 * id + 2);
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
    u_int16_t available = end_free - (num_records + 1) * 4;
    return size <= available;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u_int16_t shift = end - start;
    if (shift == 0) {
        return;
    }
    memcpy(address(end_free + 1 + shift), address(end_free + 1), start - end_free + 1);
    for(RecordID id: *ids()) {
        u_int16_t size;
        u_int16_t loc;
        get_header(size, loc, id);
        if(loc <= start) {
            loc += shift;
            put_header(id, size, loc);
        }
    }
    end_free += shift;
    put_header();
}


u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t *)this->address(offset);
}

void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t *)this->address(offset) = n;
}

void* SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// **************
// * HEAP FILE *
// **************

void HeapFile::create(void){
	this->db_open(DB_CREATE); //doesn't pass flags like python code, header doesnt accept them
	SlottedPage* block = this->get_new();
	this->put(block);
}

void HeapFile::drop() {
    // Not Implemented
}

void HeapFile::open(void){
	this->db_open(DB_CREATE);
>>>>>>> 1e47a1fdf71ba9904084c9965c45518693eb8bc0
	// HeapFile doesn't have a block_size variable, python line not included
	//this->block_size = this->stat['re_len'];

}

void HeapFile::close(void){
        u_int32_t i = 0;
		this->db.close(i);
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
    Dbt* key = new Dbt();
    Dbt* data = new Dbt();
    void* id = &block_id;
    key->set_data(id);
    this->db.get(NULL, key, data, 0);
    SlottedPage* retPage = new SlottedPage(*data, block_id, false);
	return retPage;
}


void HeapFile::put(DbBlock *block){
    Dbt* key = new Dbt();
    BlockID blockID = block->get_block_id();
    key->set_data(&blockID);
	this->db.put(NULL, key, block->get_block(), DB_APPEND);//not sure if bytes is a function
}

BlockIDs* HeapFile::block_ids(){
	BlockIDs* blockIDs = new BlockIDs();
	for (u_int32_t i = 1; i <= this->last; i++){
		blockIDs->push_back(i);
	}
	return blockIDs;
}

<<<<<<< HEAD
u_int32_t HeapFile::get_last_block_id() { 
	return last; 
}

//confused about this one
void HeapFile::db_open(uint flags){
	if (!this->closed){
		return;
	}
	this->db.set_re_len(DbBlock::BLOCK_SZ); //does this function exist?
	this->dbfilename = this->name + ".db"; //what is this->name?
	this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);
    if (flags == 0) {
        DB_BTREE_STAT stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        this->last = stat.bt_ndata;
    } else {
        this->last = 0;
    }
	this->closed = false;
}

// **************
// * HEAP TABLE *
// **************

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                               ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name) {

}

void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch (const DbRelationError &) {
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

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    // Not implemented
}

void HeapTable::del(const Handle handle) {
    // Not Implemented
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
    DbBlock* block = this->file.get(this->file.get_last_block_id());
    RecordID recordId;
    try {
        recordId = block->add(data);
    } catch (const DbRelationError &) {
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
        string* dataString = (string*)(data->get_data());
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            string subDataString = dataString->substr(offset, offset + sizeof(int32_t)); // from offset => offset + sizeof(int32_t);
            const void* subDataPointer = &subDataString;
            cout << subDataString;
            int v;
            memcpy(&v, subDataPointer, sizeof(int32_t));
            Value* val = new Value(v);
            retRow->insert(pair<Identifier , Value>(column_name, *val));
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            string sizeDataString = dataString->substr(offset, offset + sizeof(u_int16_t)); // Make Constant for 2
            const void* sizeDataPointer = &sizeDataString;
            int size;
            memcpy(&size, sizeDataPointer, sizeof(u_int16_t));
            offset += 2;
            string subDataString = dataString->substr(offset, offset + size);
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

bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}
