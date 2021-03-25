/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "postgres.h"

#include <time.h>

#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "catalog/catalog.h"
#include "commands/progress.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include <utils/typcache.h>

#include "compression/compress_uk/compress_uk_utils.h"

/*static bool _bt_compare_scankey_args(IndexScanDesc scan, ScanKey op, ScanKey leftarg,
									 ScanKey rightarg, bool *result);
static bool _bt_fix_scankey_strategy(ScanKey skey, int16 *indoption);
static void _bt_mark_scankey_required(ScanKey skey);
static bool _bt_check_rowcompare(ScanKey skey, IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
								 ScanDirection dir, bool *continuescan);
static int _bt_keep_natts(Relation rel, IndexTuple lastleft, IndexTuple firstright,
						  BTScanInsert itup_key);
*/
/*
 * compress_uk_mkscankey
 *		Build an insertion scan key that contains comparison data from itup
 *		as well as comparator routines appropriate to the key datatypes.
 *
 *		When itup is a non-pivot tuple, the returned insertion scan key is
 *		suitable for finding a place for it to go on the leaf level.  Pivot
 *		tuples can be used to re-find leaf page with matching high key, but
 *		then caller needs to set scan key's pivotsearch field to true.  This
 *		allows caller to search for a leaf page with a matching high key,
 *		which is usually to the left of the first leaf page a non-pivot match
 *		might appear on.
 *
 *		The result is intended for use with _bt_compare() and _bt_truncate().
 *		Callers that don't need to fill out the insertion scankey arguments
 *		(e.g. they use an ad-hoc comparison routine, or only need a scankey
 *		for _bt_truncate()) can pass a NULL index tuple.  The scankey will
 *		be initialized as if an "all truncated" pivot tuple was passed
 *		instead.
 *
 *		Note that we may occasionally have to share lock the metapage to
 *		determine whether or not the keys in the index are expected to be
 *		unique (i.e. if this is a "heapkeyspace" index).  We assume a
 *		heapkeyspace index when caller passes a NULL tuple, allowing index
 *		build callers to avoid accessing the non-existent metapage.  We
 *		also assume that the index is _not_ allequalimage when a NULL tuple
 *		is passed; CREATE INDEX callers call _bt_allequalimage() to set the
 *		field themselves.
 */
BTScanInsert
compress_uk_mkscankey(Relation rel, TupleDesc itupdesc, IndexTuple itup)
{
	BTScanInsert key;
	ScanKey skey;
	// TupleDesc	itupdesc;
	int indnkeyatts;
	int16 *indoption;
	int tupnatts;
	int i;

	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	indoption = rel->rd_indoption;
	tupnatts = itup ? BTreeTupleGetNAtts(itup, rel) : 0;

	Assert(tupnatts <= IndexRelationGetNumberOfAttributes(rel));

	/*
	 * We'll execute search using scan key constructed on key columns.
	 * Truncated attributes and non-key attributes are omitted from the final
	 * scan key.
	 */
	key = palloc(offsetof(BTScanInsertData, scankeys) + sizeof(ScanKeyData) * indnkeyatts);
	if (itup)
		_bt_metaversion(rel, &key->heapkeyspace, &key->allequalimage);
	else
	{
		/* Utility statement callers can set these fields themselves */
		key->heapkeyspace = true;
		key->allequalimage = false;
	}
	key->anynullkeys = false; /* initial assumption */
	key->nextkey = false;
	key->pivotsearch = false;
	key->keysz = Min(indnkeyatts, tupnatts);
	key->scantid = key->heapkeyspace && itup ? BTreeTupleGetHeapTID(itup) : NULL;
	skey = key->scankeys;
	for (i = 0; i < indnkeyatts; i++)
	{
		FmgrInfo procinfo;
		Datum arg;
		bool null;
		int flags;
		TypeCacheEntry *tce;
		Oid attype;
		Oid order_proc_oid;
		/*
		 * Key arguments built from truncated attributes (or when caller
		 * provides no tuple) are defensively represented as NULL values. They
		 * should never be used.
		 */
		if (i < tupnatts)
			arg = index_getattr(itup, i + 1, itupdesc, &null);
		else
		{
			arg = (Datum) 0;
			null = true;
		}
		/* get comparison function for the type. itupdesc has
		 * descriptor for entries in the btree leaf nodes
		 */
		Form_pg_attribute attr = TupleDescAttr(itupdesc, i);
		attype = attr->atttypid;
		tce = lookup_type_cache(attype, TYPECACHE_BTREE_OPFAMILY);
		order_proc_oid = get_opfamily_proc(tce->btree_opf, attype, attype, BTORDER_PROC);
		/* where should this be alloced TODOin rd_amcache and
		 * use rd_indexcxt. Should save this in State and then retrieve
		 * from there. TODO !!!!!!!!!!!!!!!!!
		 */
		fmgr_info_cxt(order_proc_oid, &procinfo, rel->rd_indexcxt);

		flags = (null ? SK_ISNULL : 0) | (indoption[i] << SK_BT_INDOPTION_SHIFT);
		ScanKeyEntryInitializeWithInfo(&skey[i],
									   flags,
									   (AttrNumber)(i + 1),
									   InvalidStrategy,
									   InvalidOid,
									   attr->attcollation, // TODO should we instead pass in no
														   // collation here and not support it at
														   // all???
									   &procinfo,
									   arg);
		/* Record if any key attribute is NULL (or truncated) */
		if (null)
			key->anynullkeys = true;
	}

	return key;
}
