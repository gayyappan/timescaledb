/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/amapi.h>
#include <access/nbtree.h>
#include <catalog/index.h>
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/typcache.h"

#include "compression/compression.h"
#include "compress_uk.h"
#include "compress_uk_build.h"
#include "compress_uk_insert.h"

Datum
tsl_compress_uk_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber; /* support for <, > = et.c */
	amroutine->amsupport = 1;					   // BTOrder-Proc needed
	amroutine->amoptsprocnum = 0;				   // BTOPTIONS_PROC;
	amroutine->amcanorder =
		false; /* figure out how to do this. this is for unique index . so yes? TODO */
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false; // false for now
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = false; // what does true mean TODO overrid btree
	amroutine->amsearcharray = false; // cannot compare against an array
	amroutine->amsearchnulls = true;  // any special support needed?
	amroutine->amstorage = true;	  /*index storage datatype differs from data type*/
	amroutine->amclusterable = false; // should this be true ? this is a unique index. TODO
	amroutine->ampredlocks = false;   // TODO what does this mean? CHECK this.
									  /* other indexes seem to support this */
	amroutine->amcanparallel = false; // should this work? TODO
	amroutine->amcaninclude = false;  /* override as false */
	amroutine->amusemaintenanceworkmem = false; // same as btree
	amroutine->amparallelvacuumoptions = 0;		// no support now
	// amroutine->amparallelvacuumoptions =
	//	VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;

	amroutine->ambuild = compress_uk_build;
	amroutine->ambuildempty = compress_uk_buildempty;
	amroutine->aminsert = compress_uk_insert;
	amroutine->ambulkdelete = compress_uk_bulkdelete;
	amroutine->amvacuumcleanup = compress_uk_vacuumcleanup;
	/* index scan specific functions : TODO */
	amroutine->amcanreturn = NULL;						  /* no index scan support */
	amroutine->amcostestimate = compress_uk_costestimate; // needed by get_relation_info

	amroutine->amoptions =
		btoptions; // TODO. we have no option. ButDEfineIndex expects a non null value eheres
	amroutine->amproperty = NULL; // TODO do we need this?
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = NULL; // TODO do we need this?
	amroutine->ambeginscan = compress_uk_beginscan;
	amroutine->amrescan = compress_uk_rescan;
	amroutine->amgettuple = compress_uk_gettuple;
	amroutine->amgetbitmap = NULL; // no support for bitmap scan
	amroutine->amendscan = compress_uk_endscan;

	amroutine->ammarkpos = NULL;			  /* no support for ordered scans */
	amroutine->amrestrpos = NULL;			  /* no support for ordered scans */
	amroutine->amestimateparallelscan = NULL; /* no support for parallel scans */
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/* interface functions */
//    ambuild_function ambuild;
//    ambuildempty_function ambuildempty;
//    aminsert_function aminsert;
//    ambulkdelete_function ambulkdelete;
//    amvacuumcleanup_function amvacuumcleanup;
//    amcanreturn_function amcanreturn;   /* can be NULL */
//    amcostestimate_function amcostestimate;
//    amoptions_function amoptions;
//    amproperty_function amproperty;     /* can be NULL */
//    ambuildphasename_function ambuildphasename;   /* can be NULL */
//    amvalidate_function amvalidate;
//    ambeginscan_function ambeginscan;
//    amrescan_function amrescan;
//    amgettuple_function amgettuple;     /* can be NULL */
//    amgetbitmap_function amgetbitmap;   /* can be NULL */
//    amendscan_function amendscan;
//    ammarkpos_function ammarkpos;       /* can be NULL */
//    amrestrpos_function amrestrpos;     /* can be NULL */
//  ----> from PG docs

typedef struct CompressUkIndexState
{
	IndexDecompressor *index_decompressor;
	Relation compress_index_rel;
	/* Tuple format saved in the index.
	 * This does not match the passed in index rel
	 * tuple desc. So we need to store this separately.
	 */
	TupleDesc index_tuple_desc; /* tuple format saved in the index .*/
} CompressUKIndexState;

static void compress_uk_init_state(CompressUKIndexState *state, Relation index_rel,
								   Relation heap_rel);

/* we have the compressed table definition here.
 * identify the compressed columns and the non compressed ones
 */
static void
compress_uk_init_state(CompressUKIndexState *state, Relation index_rel, Relation heap_rel)
{
	state->index_decompressor = index_decompressor_create(index_rel, heap_rel);
	state->index_tuple_desc = index_decompressor_get_out_desc(state->index_decompressor);
}

void
compress_uk_buildempty(Relation index_rel)
{
	btbuildempty(index_rel);
}

/*this is a row from the compressed chunk . Translate to uncompressed form
 * and insert into btree
 */
bool
compress_uk_insert(Relation index_rel, Datum *values, bool *isnull, ItemPointer ht_ctid,
				   Relation heap_rel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
	bool result = false;
	Datum *out_val;
	bool *out_null;
	CompressUKIndexState *ukstate = (CompressUKIndexState *) indexInfo->ii_AmCache;
	IndexDecompressor *index_decompressor;
	if (ukstate == NULL)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(indexInfo->ii_Context);
		ukstate = (CompressUKIndexState *) palloc(sizeof(CompressUKIndexState));
		compress_uk_init_state(ukstate, index_rel, heap_rel);
		indexInfo->ii_AmCache = (void *) ukstate;
		MemoryContextSwitchTo(oldCtx);
	}
	index_decompressor = ukstate->index_decompressor;
	while (index_decompressor_get_next(index_decompressor,
									   values,
									   isnull,

									   &out_val,
									   &out_null))
	{
		IndexTuple itup;

		/* generate an index tuple, try to resus here TODO */
		itup = index_form_tuple(ukstate->index_tuple_desc, values, isnull);
		itup->t_tid = *ht_ctid;

		result = ts_compress_uk_doinsert(index_rel,
										 ukstate->index_tuple_desc,
										 itup,
										 checkUnique,
										 heap_rel);
		pfree(itup);
	}
	/* return val is useful only for UNIQUE_CHECK_PARTIAL. Not supported at the moment.
	 * Check_bt_do_insert comments */
	return result;
}

typedef struct CompressUKScanOpaque
{
	BTScanOpaque so;
	IndexDecompressor *index_decompressor;
	TupleDesc index_tuple_desc; /* tuple format saved in the index .*/
} CompressUKScanOpaque;

/* TODO: HACK redirect to btbeginscan with the dummy rel
 * tupledesc has to eb set up to read tuples from the index on the compressed
 * data.
 */
/* based on btbeginscan */
IndexScanDesc
compress_uk_beginscan(Relation index_rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	CompressUKScanOpaque *cukscan;
	BTScanOpaque so;
	Oid compress_relid = IndexGetRelation(RelationGetRelid(index_rel), false);
	Relation compress_rel = table_open(compress_relid, AccessShareLock);

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(index_rel, nkeys, norderbys);

	/* allocate private workspace */
	cukscan = (CompressUKScanOpaque *) palloc(sizeof(CompressUKScanOpaque));
	so = (BTScanOpaque) palloc(sizeof(BTScanOpaqueData));
	cukscan->so = so;
	cukscan->index_decompressor = index_decompressor_create(index_rel, compress_rel);
	cukscan->index_tuple_desc = index_decompressor_get_out_desc(cukscan->index_decompressor);
	BTScanPosInvalidate(so->currPos);
	BTScanPosInvalidate(so->markPos);
	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	so->arrayKeyData = NULL; /* assume no array keys for now */
	so->numArrayKeys = 0;
	so->arrayKeys = NULL;
	so->arrayContext = NULL;

	so->killedItems = NULL; /* until needed */
	so->numKilled = 0;

	/*
	 * We don't know yet whether the scan will be index-only, so we do not
	 * allocate the tuple workspace arrays until btrescan.  However, we set up
	 * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
	 */
	so->currTuples = so->markTuples = NULL;

	scan->xs_itupdesc = index_decompressor_get_out_desc(cukscan->index_decompressor);

	scan->opaque = cukscan;

	return scan;
}

/*
 *  copy of btrescan
 */
void
compress_uk_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys, ScanKey orderbys,
				   int norderbys)
{
	CompressUKScanOpaque *cukscan = (CompressUKScanOpaque *) scan->opaque;
	BTScanOpaque so = cukscan->so;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (BTScanPosIsValid(so->currPos))
	{
		/* Before leaving current page, deal with any killed items */
		if (so->numKilled > 0)
			_bt_killitems(scan);
		BTScanPosUnpinIfPinned(so->currPos);
		BTScanPosInvalidate(so->currPos);
	}

	so->markItemIndex = -1;
	so->arrayKeyCount = 0;
	BTScanPosUnpinIfPinned(so->markPos);
	BTScanPosInvalidate(so->markPos);

	/*
	 * Allocate tuple workspace arrays, if needed for an index-only scan and
	 * not already done in a previous rescan call.  To save on palloc
	 * overhead, both workspaces are allocated as one palloc block; only this
	 * function and btendscan know that.
	 *
	 * NOTE: this data structure also makes it safe to return data from a
	 * "name" column, even though btree name_ops uses an underlying storage
	 * datatype of cstring.  The risk there is that "name" is supposed to be
	 * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
	 * However, since we only return data out of tuples sitting in the
	 * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
	 * data out of the markTuples array --- running off the end of memory for
	 * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
	 * adding special-case treatment for name_ops elsewhere.
	 */
	if (scan->xs_want_itup && so->currTuples == NULL)
	{
		so->currTuples = (char *) palloc(BLCKSZ * 2);
		so->markTuples = so->currTuples + BLCKSZ;
	}

	/*
	 * Reset the scan keys. Note that keys ordering stuff moved to _bt_first.
	 * - vadim 05/05/97
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData, scankey, scan->numberOfKeys * sizeof(ScanKeyData));
	so->numberOfKeys = 0; /* until _bt_preprocess_keys sets it */

	/* If any keys are SK_SEARCHARRAY type, set up array-key info */
	_bt_preprocess_array_keys(scan);
}

/* copied from btendscan */
void
compress_uk_endscan(IndexScanDesc scan)
{
	CompressUKScanOpaque *cukscan = (CompressUKScanOpaque *) scan->opaque;
	BTScanOpaque so = cukscan->so;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (BTScanPosIsValid(so->currPos))
	{
		/* Before leaving current page, deal with any killed items */
		if (so->numKilled > 0)
			_bt_killitems(scan);
		BTScanPosUnpinIfPinned(so->currPos);
	}

	so->markItemIndex = -1;
	BTScanPosUnpinIfPinned(so->markPos);

	/* No need to invalidate positions, the RAM is about to be freed. */

	/* Release storage */
	if (so->keyData != NULL)
		pfree(so->keyData);
	/* so->arrayKeyData and so->arrayKeys are in arrayContext */
	if (so->arrayContext != NULL)
		MemoryContextDelete(so->arrayContext);
	if (so->killedItems != NULL)
		pfree(so->killedItems);
	if (so->currTuples != NULL)
		pfree(so->currTuples);
	/* so->markTuples should not be pfree'd, see btrescan */
	pfree(so);
	pfree(cukscan); // should indexdecompressor be alloced and freed as well TODO??
}

/* based on btgettuple */
bool
compress_uk_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	CompressUKScanOpaque *cukscan = (CompressUKScanOpaque *) scan->opaque;
	BTScanOpaque so = (BTScanOpaque) cukscan->so;
	bool res;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !BTScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_bt_start_array_keys(scan, dir);
	}

	/* This loop handles advancing to the next array elements, if any */
	do
	{
		/*
		 * If we've already initialized this scan, we can just advance it in
		 * the appropriate direction.  If we haven't done so yet, we call
		 * _bt_first() to get the first item in the scan.
		 */
		if (!BTScanPosIsValid(so->currPos))
			res = _bt_first(scan, dir);
		else
		{
			/*
			 * Check to see if we should kill the previously-fetched tuple.
			 */
			if (scan->kill_prior_tuple)
			{
				/*
				 * Yes, remember it for later. (We'll deal with all such
				 * tuples at once right before leaving the index page.)  The
				 * test for numKilled overrun is not just paranoia: if the
				 * caller reverses direction in the indexscan then the same
				 * item might get entered multiple times. It's not worth
				 * trying to optimize that, so we don't detect it, but instead
				 * just forget any excess entries.
				 */
				if (so->killedItems == NULL)
					so->killedItems = (int *) palloc(MaxTIDsPerBTreePage * sizeof(int));
				if (so->numKilled < MaxTIDsPerBTreePage)
					so->killedItems[so->numKilled++] = so->currPos.itemIndex;
			}
			/*
			 * Now continue the scan.
			 */
			res = _bt_next(scan, dir);
		}

		/* If we have a tuple, return it ... */
		if (res)
			break;
		/* ... otherwise see if we have more array keys to deal with */
	} while (so->numArrayKeys && _bt_advance_array_keys(scan, dir));

	return res;
}

IndexBulkDeleteResult *
compress_uk_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
					   IndexBulkDeleteCallback callback, void *callback_state)
{
	return NULL;
}

IndexBulkDeleteResult *
compress_uk_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return NULL;
}

// see if we can reuse btcostestimate
// give soem absurd value for now
void
compress_uk_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
						 Cost *indexStartupCost, Cost *indexTotalCost,
						 Selectivity *indexSelectivity, double *indexCorrelation,
						 double *indexPages)
{
	*indexStartupCost = 100000;
	*indexTotalCost = 1000000.0;
	*indexSelectivity = 0;
	*indexCorrelation = 0.0;
	*indexPages = 800;
}
