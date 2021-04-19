#include "heap_storage.h"
#include "storage_engine.h"

using namespace std;

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

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    ValueDict* row = this->project(handle);
    // STOPPED HERE!
}



void HeapTable::drop() {
    delete this->file;
}



bool test_heap_storage() {return true;}


