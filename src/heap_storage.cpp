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
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    }
    else
    {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }

    put_header();
    put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

RecordIDs *SlottedPage::ids(void)
{
    u16 size, loc;
    RecordIDs *ids = new RecordIDs();

    for (RecordID i = 1; i <= this->num_records; i++)
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

    memmove(this->address(this->end_free + shift + 1), address(this->end_free + 1), shift);

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
    void *addr = (void *)((char *)this->block.get_data() + offset);
    // std::cout << "block data address: " << this->block.get_data()
    //           << ", offset: " << offset
    //           << ", calculated address: " << addr << std::endl;
    return addr;
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4 * id, size);
    put_n((u16)4 * id + 2, loc);
}

//------------------------HeapFile----------------------------------------------

void HeapFile::create(void)
{
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *blockPage = this->get_new();
    this->put(blockPage);
    delete blockPage;
}

// FIXME
void HeapFile::drop(void)
{
    this->close();
    // Db db(_DB_ENV, 0);
    // db.remove((this->dbfilename+".db").c_str(), nullptr, 0);
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
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    this->last = bt_ndata;
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
    this->db.put(nullptr, &key, block->get_block(), 0); // txnid is null
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
    catch (const DbException &e)
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
    this->open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
    // FIXME
}

void HeapTable::del(const Handle handle)
{
    // FIXME
}

Handles *HeapTable::select()
{
    return select(nullptr);
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
    return project(handle, &this->column_names);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    BlockID blockID = handle.first;
    RecordID recordID = handle.second;
    SlottedPage *block = this->file.get(blockID);
    Dbt *data = block->get(recordID);
    ValueDict *rows = unmarshal(data);
    if (column_names == nullptr || column_names->empty())
        return rows;

    // Create a new ValueDict for the filtered columns.
    ValueDict *filteredRows = new ValueDict;
    for (const auto &colName : *column_names)
    {
        auto it = rows->find(colName);
        if (it != rows->end())
        {
            (*filteredRows)[colName] = it->second;
        }
    }
    delete rows;
    delete block;
    delete data;
    return filteredRows;
}

ValueDict *HeapTable::validate(const ValueDict *row)
{
    ValueDict *fullRow = new ValueDict();

    for (const auto &column_name : this->column_names)
    {
        auto it = row->find(column_name);

        // Check if the column name exists in the input row
        if (it == row->end())
        {
            delete fullRow;
            throw DbRelationError("Column '" + column_name + "' is missing in the row.");
        }
        (*fullRow)[column_name] = it->second;
    }
    return fullRow;
}

Handle HeapTable::append(const ValueDict *row)
{
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID id;
    try
    {
        id = block->add(data);
    }
    catch (DbRelationError const &)
    {
        block = this->file.get_new();
        id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *)data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), id);
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
    char *bytes = static_cast<char *>(data->get_data());
    uint offset = 0;
    uint col_num = 0;

    ValueDict *row = new ValueDict();
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];

        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            // Read an integer value
            int32_t value = *(reinterpret_cast<int32_t *>(bytes + offset));
            offset += sizeof(int32_t);
            (*row)[column_name] = Value(value);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            u16 size = *(reinterpret_cast<u16 *>(bytes + offset));
            offset += sizeof(u16);
            std::string value(bytes + offset, size);
            offset += size;
            (*row)[column_name] = Value(value);
        }
        else
        {
            delete row;
            throw DbRelationError("Unsupported data type found");
        }
    }
    return row;
}

// test function -- returns true if all tests pass
bool test_heap_storage()
{
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
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exists ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;

    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    if (result == nullptr)
    {
        std::cerr << "Project returned null." << std::endl;
        return false;
    }
    Value value = (*result)["a"];
    if (value.n != 12)
    {
        std::cout << "failed here because value.n " << value.n << std::endl;
        return false;
    }
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();
    return true;
}