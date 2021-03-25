/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_INSERT_H
#define TIMESCALEDB_TSL_COMPRESS_UK_INSERT_H

#include <postgres.h>
#include <fmgr.h>

/* THis is a HACK> REmove this code */

#include <postgres.h>

#include "catalog.h"

bool ts_compress_uk_doinsert(Relation index_rel, TupleDesc index_tupdesc, IndexTuple itup,
							 IndexUniqueCheck checkUnique, Relation heapRel);

#endif /* TIMESCALEDB_TSL_COMPRESS_UK_INSERT_H */
