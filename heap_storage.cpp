#include "heap_storage.h"
#include "storage_engine.h"

using namespace std;

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



bool test_heap_storage() {return true;}

/* FIXME FIXME FIXME */
