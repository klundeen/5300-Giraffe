#include "heap_storage.h"

class SlottedPage : public DbBlock{
public:
	BYTE_ORDER = "big";

	RecordID add(const Dbt *data)
	{
		if (!this.has_room(len(data) + 4)
		{
			raise ValueError("Not enough room in block");
		}
		this.num_records += 1;
		u_int16_t id = this.num_records;
		u_int16_t size = len(data);
		self.end_free -= size;
		u_int16_t loc = this.end_free + 1;
		this.put_header();
	}

	Dbt* get(RecordID record_id)
	{
		u_int16_t size;
		u_int16_t loc;
		this.get_header(&size, &loc, record_id);
		if (loc == 0)
		{
			return NULL;
		}
		return this.block[loc:loc + size]; //python code
	}

protected:
	u_int16_t num_records;
	u_int16_t end_free;


	void get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
	{
		size = this.get_n(4 * id);
		loc = this.get_n(4 * id + 2);
		return;
	}
	
	
	void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0)

	u_int16_t get_n(u_int16_t offset)
	{
		return int.from_bytes(this.block[offset:offset + 2], byteorder=self.BYTE_ORDER) //python code
	}



bool test_heap_storage() {return true;}
/* FIXME FIXME FIXME */
