/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_UTILS_H
#define TIMESCALEDB_TSL_COMPRESS_UK_UTILS_H

#include <postgres.h>
#include <fmgr.h>

/* THis is a HACK> REmove this code */

#include <postgres.h>

#include "catalog.h"

BTScanInsert compress_uk_mkscankey(Relation rel, TupleDesc itupdesc, IndexTuple itup);

#endif /* TIMESCALEDB_TSL_COMPRESS_UK_UTILS_H */
