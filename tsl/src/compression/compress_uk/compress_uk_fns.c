/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "compress_uk_fns.h"

Datum
tsl_compress_uk_equal(PG_FUNCTION_ARGS)
{
	bool ret_val = 1;
	PG_RETURN_BOOL(ret_val);
}

Datum
tsl_compress_uk_cmp(PG_FUNCTION_ARGS)
{
	int ret_val = 1;
	PG_RETURN_BOOL(ret_val);
}
