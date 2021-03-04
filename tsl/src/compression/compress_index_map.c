/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/* THis is a HACK> REmove this code */

#include <postgres.h>
#include <utils/fmgroids.h>

//#include "hypertable.h"
//#include "hypertable_cache.h"
#include "catalog.h"
#include "compress_index_map.h"
#include "scanner.h"
#include "scan_iterator.h"

/*
static void
chunk_index_map_fill_from_tuple(FormData_chunk_index_map *fd, TupleInfo *ti)
{
	Datum values[Natts_chunk_index_map];
	bool isnulls[Natts_chunk_index_map];
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnulls);

	Assert(!isnulls[AttrNumberGetAttrOffset(Anum_chunk_index_map_orig_chunk_index)]);
	Assert(!isnulls[AttrNumberGetAttrOffset(Anum_chunk_index_map_compress_chunk_index)]);
	memcpy(&fd->orig_chunk_index,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_index_map_orig_chunk_index)]),
		   NAMEDATALEN);
	memcpy(&fd->compress_chunk_index,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_index_map_compress_chunk_index)]),
		   NAMEDATALEN);
	if (should_free)
		heap_freetuple(tuple);
}
*/

TSDLLEXPORT void
ts_chunk_index_map_fill_tuple_values(FormData_chunk_index_map *fd, Datum *values, bool *nulls)
{
	memset(nulls, 0, sizeof(bool) * Natts_chunk_index_map);
	values[AttrNumberGetAttrOffset(Anum_chunk_index_map_orig_chunk_index)] =
		NameGetDatum(&fd->orig_chunk_index);
	values[AttrNumberGetAttrOffset(Anum_chunk_index_map_compress_chunk_index)] =
		NameGetDatum(&fd->compress_chunk_index);
}

/* returns length of list and fills passed in list with pointers
 * to FormData_chunk_index_map
 */
char *
ts_chunk_index_map_get(char *compress_index_name)
{
	Name orig_chunk_index;
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_INDEX_MAP, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK_INDEX_MAP, CHUNK_INDEX_MAP_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_chunk_index_map_pkey_compress_chunk_index,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   CStringGetDatum(compress_index_name));

	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		Datum datum = slot_getattr(ts_scan_iterator_slot(&iterator),
								   Anum_chunk_index_map_orig_chunk_index,
								   &isnull);

		Assert(!isnull);
		orig_chunk_index = DatumGetName(datum);
	}
	return NameStr(*orig_chunk_index);
}

/*
TSDLLEXPORT bool
ts_chunk_index_map_delete_by_hypertable_id(int32 htid)
{
	int count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_INDEX_MAP, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), CHUNK_INDEX_MAP, CHUNK_INDEX_MAP_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_chunk_index_map_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	return count > 0;
}
*/
