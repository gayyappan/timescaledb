/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_FNS_H
#define TIMESCALEDB_TSL_COMPRESS_UK_FNS_H

#include <postgres.h>
#include <fmgr.h>

extern Datum tsl_compress_uk_equal(PG_FUNCTION_ARGS);
extern Datum tsl_compress_uk_cmp(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_COMPRESS_UK_FNS_H */
