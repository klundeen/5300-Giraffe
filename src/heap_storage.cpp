#include "heap_storage.h"
#include "storage_engine.h"
#include <cstring>  


typedef u_int16_t u16;

//------------------------SlottedPage----------------------------------------------

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt *SlottedPage::get(RecordID record_id)
{
    u16 size, loc;
    this->get_header(size, loc, record_id);
    if (loc == 0)
    {
        return nullptr;
    }
    return new Dbt(address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    u16 extra = 0;
    if (new_size > size)
    {
        extra = new_size - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        // need to shift records to make room for new data
        slide(loc, loc - new_size);
        memcpy(this->address(loc - new_size), data.get_data(), new_size);
    }
    else
    {
        memcpy(this->address(loc), data.get_data(), new_size);
    }

    put_header();
    put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id)
{
    // Empty implementation
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

RecordIDs *SlottedPage::ids(void)
{
    u16 size, loc;
    RecordIDs *ids = new RecordIDs;

    for (RecordID i = 1; i < this->num_records; i++)
    {
        get_header(size, loc, i);
        if (loc == 0) // tombstone
            continue;
        ids->push_back(i);
    }

    return ids;
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id)
{
    if (id > num_records)
        throw("Record id is not valid: " + id);

    size = get_n(4 * id);
    loc = get_n((4 * id) + 2);
}

bool SlottedPage::has_room(u16 size)
{
    return size <= this->end_free - (this->num_records + 1) * 4;
}

void SlottedPage::slide(u16 start, u16 end)
{
    u16 shift = end - start;
    if (shift == 0)
        return;

    memcpy(this->address(this->end_free + shift + 1), address(this->end_free + 1), shift);

    RecordIDs *ids = this->ids();
    for (RecordID &id : *ids)
    {
        u16 size, loc;
        get_header(size, loc, id);
        if (loc <= start)
        {
            loc += shift;
            put_header(id, size, loc);
        }
    }
    end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

//------------------------HeapFile----------------------------------------------

void HeapFile::create(void)
{
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *blockPage = this->get_new();
    this->put(blockPage);
}

// FIXME
void HeapFile::drop(void)
{
    this->close();
    std::remove(this->dbfilename.c_str());
}

// FIXME
void HeapFile::open(void)
{
    this->db_open();
}

// FIXME
void HeapFile::db_open(uint flags)
{
    if (closed == false)
        return; // no need to do anything

    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";
    db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644); // RECNO is a record number database, 0644 is unix file permission
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat.bt_ndata;

    this->closed = false;
}

void HeapFile::close(void)
{
    this->db.close(0);
    this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new(void)
{
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage *HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock *block)
{
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), DB_APPEND); // txnid is null
}

BlockIDs *HeapFile::block_ids()
{
    BlockIDs *blockIds = new BlockIDs;
    for (BlockID i = 1; i <= (BlockID)this->last; i++)
    {
        blockIds->push_back(i);
    }
    return blockIds;
}

//------------------------HeapTable----------------------------------------------
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), // Initialize base class
      file(table_name)                                         // Initialize member variable file
{
}

void HeapTable::create()
{
    this->file.create();
}

void HeapTable::create_if_not_exists()
{
    try
    {
        this->open();
    }
    catch (const DbException& e) // FIXME
    {
        this->create();
    }
}

void HeapTable::drop()
{
    this->file.drop();
}

void HeapTable::open()
{
    this->file.open();
}

void HeapTable::close()
{
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row)
{
    // Empty implementation
    return Handle(); // Placeholder return value
}

void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
    // Empty implementation
}

void HeapTable::del(const Handle handle)
{
    // Empty implementation
}

Handles *HeapTable::select()
{
    // Empty implementation
    return nullptr; // Placeholder return value
}

Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict *HeapTable::project(Handle handle)
{
    // Empty implementation
    return nullptr; // Placeholder return value
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    // Empty implementation
    return nullptr; // Placeholder return value
}

ValueDict *HeapTable::validate(const ValueDict *row)
{
    // Empty implementation
    return nullptr; // Placeholder return value
}

Handle HeapTable::append(const ValueDict *row)
{
    // Empty implementation
    return Handle(); // Placeholder return value
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
    // Empty implementation
    return nullptr; // Placeholder return value
}

bool test_heap_storage()
{
    // Empty implementation
    return false; // Placeholder return value
}
