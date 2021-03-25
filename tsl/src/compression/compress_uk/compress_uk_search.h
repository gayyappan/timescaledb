/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_SEARCH_H
#define TIMESCALEDB_TSL_COMPRESS_UK_SEARCH_H

#include <postgres.h>

extern BTStack ts_bt_search(Relation rel, TupleDesc rel_tupdesc, BTScanInsert key, Buffer *bufP,
							int access, Snapshot snapshot);
extern Buffer ts_bt_moveright(Relation rel, TupleDesc rel_tupdesc, BTScanInsert key, Buffer buf,
							  bool forupdate, BTStack stack, int access, Snapshot snapshot);
extern OffsetNumber ts_bt_binsrch_insert(Relation rel, TupleDesc rel_tupdesc,
										 BTInsertState insertstate);
extern int32 ts_bt_compare(Relation rel, TupleDesc rel_tupdesc, BTScanInsert key, Page page,
						   OffsetNumber offnum);
extern bool ts_bt_first(IndexScanDesc scan, TupleDesc rel_tupdesc, ScanDirection dir);
extern bool ts_bt_next(IndexScanDesc scan, ScanDirection dir);
extern Buffer _bt_get_endpoint(Relation rel, uint32 level, bool rightmost, Snapshot snapshot);

#endif /* TIMESCALEDB_TSL_COMPRESS_UK_SEARCH_H */
