/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESS_UK_H
#define TIMESCALEDB_TSL_COMPRESS_UK_H

#include <postgres.h>
#include <access/amapi.h>
#include <nodes/execnodes.h>

extern Datum tsl_compress_uk_handler(PG_FUNCTION_ARGS);

// TODO should these functions be internal to compress_uk.c???
void compress_uk_buildempty(Relation index);
IndexBuildResult *compress_uk_build(Relation heap, Relation index, IndexInfo *indexInfo);

extern bool compress_uk_insert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
							   Relation heapRel, IndexUniqueCheck checkUnique,
							   IndexInfo *indexInfo);
IndexScanDesc compress_uk_beginscan(Relation rel, int nkeys, int norderbys);
void compress_uk_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys, ScanKey orderbys,
						int norderbys);

void compress_uk_endscan(IndexScanDesc scan);
bool compress_uk_gettuple(IndexScanDesc scan, ScanDirection dir);

IndexBulkDeleteResult *compress_uk_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
											  IndexBulkDeleteCallback callback,
											  void *callback_state);
IndexBulkDeleteResult *compress_uk_vacuumcleanup(IndexVacuumInfo *info,
												 IndexBulkDeleteResult *stats);
void compress_uk_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
							  Cost *indexStartupCost, Cost *indexTotalCost,
							  Selectivity *indexSelectivity, double *indexCorrelation,
							  double *indexPages);
#endif /* TIMESCALEDB_TSL_COMPRESS_UK_H */
