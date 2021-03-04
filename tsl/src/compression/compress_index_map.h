/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_INDEX_MAP_H
#define TIMESCALEDB_TSL_COMPRESS_INDEX_MAP_H

#include <postgres.h>
#include <fmgr.h>

/* THis is a HACK> REmove this code */

#include <postgres.h>

#include "catalog.h"

void ts_chunk_index_map_fill_tuple_values(FormData_chunk_index_map *fd, Datum *values, bool *nulls);
char *ts_chunk_index_map_get(char *index_name);

#endif /* TIMESCALEDB_TSL_COMPRESS_INDEX_MAP_H */
