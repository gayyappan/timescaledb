/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_BUILD_H
#define TIMESCALEDB_TSL_COMPRESS_UK_BUILD_H

#include <postgres.h>
#include <fmgr.h>

/* THis is a HACK> REmove this code */

#include <postgres.h>

#include "catalog.h"

IndexBuildResult *
compress_uk_build(Relation heap, Relation index, IndexInfo *indexInfo);

#endif /* TIMESCALEDB_TSL_COMPRESS_UK_BUILD_H */
