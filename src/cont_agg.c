/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <catalog/pg_type.h>	  /* INT4OID */
#include <catalog/pg_aggregate.h> /*AGGKIND_xxx */
#include <catalog/toasting.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <access/xact.h> //CommandCounterIncrement
#include <access/reloptions.h>
#include <utils/syscache.h>
#include "compat.h"
#include <hypertable_cache.h>
#include <cont_agg.h>
#include <parser/parse_func.h>

void
cagg_validate_query(Query *query)
{
	Cache *hcache;
	Hypertable *ht = NULL;
	RangeTblRef *rtref = NULL;
	RangeTblEntry *rte;
	List *fromList;
	Oid relid;

	if (query->commandType != CMD_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Only SELECT query permitted for continuous aggs")));
	}
	/* check if we have a hypertable here */
	fromList = query->jointree->fromlist;
	if (list_length(fromList) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Only 1 hypertable is permitted in SELECT query permitted for continuous "
						"aggs")));
	}
	rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
	rte = list_nth(query->rtable, rtref->rtindex - 1);
	relid = rte->relid;
	if (rte->relkind == RELKIND_RELATION)
	{
		hcache = ts_hypertable_cache_pin();
		ht = ts_hypertable_cache_get_entry(hcache, relid);
		ts_cache_release(hcache);
	}
	if (ht == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Can create continuous aggregate only on hypertables")));
	}
	if (query->hasSubLinks || query->hasWindowFuncs || query->hasRecursive || query->hasTargetSRFs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid SELECT query for continuous aggregate")));
	}
	if (!query->hasAggs)
	{
		/*query can ahve aggregate without
		 * group by */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT query for continuous aggregateshould have atleast 1 aggregate "
						"function ")));
	}
	/* TODO - handle grouping sets and havingClause
	 * DISTINCT
	 */
	if (query->groupingSets)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT query for continuous aggregatecannot use aggregate function ")));
	}
	/* if we have a grouping_clause it should contain
	 * timebucket
	 */
	/*	if( query->groupClause )
		{
					foreach(l, query->groupClause)
					{
							SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
							TargetEntry *tle = get_sortgroupclause_tle(sgc,

							   query->targetList);

							opid = distinct_col_search(tle->resno, colnos, opids);
							if (!OidIsValid(opid) ||
									!equality_ops_are_compatible(opid, sgc->eqop))
									break;
					}
		}
	*/
}

/* add ts_internal_cagg_final to bytea column.
 * bytea column is the internal state for an agg. Pass info for the agg as "inp".
 * inpcol = bytea column.
 * tlist = list to which target entries are to eb added
 * tlist_attno - next available entry in targetlist
 * This function retunes an aggref
 * ts_internal_cagg_final( Oid, Oid, bytea, NULL::output_typeid)
 * //and adds entries for all the arguments to the passed in targetlist.
 * So the input targetlist is modified.
 */
static Aggref *
create_finalize_agg(Aggref *inp, Var *inpcol)
{
#define FINALFN "ts_internal_cagg_final"
	Aggref *aggref;
	HeapTuple oidtup;
	Form_pg_type oidform;
	TargetEntry *te;
	Const *oid1arg, *oid2arg, *nullarg;
	Var *bytearg;
	List *tlist = NIL; // aggregate args are stored as a list of TargetEntry
	int tlist_attno = 1;
	List *argtypes = NIL;
	Oid finalfnargtypes[] = { OIDOID, OIDOID, BYTEAOID, ANYELEMENTOID };
	Oid finalfnoid;
	List *funcname = list_make1(makeString(FINALFN));
	finalfnoid = LookupFuncName(funcname, 4, finalfnargtypes, false);
	// TODO cache finalfnoid and resue
	// The arguments are aggref oid, inputcollid, bytea column-value, NULL::returntype
	argtypes = lappend_oid(argtypes, OIDOID);
	argtypes = lappend_oid(argtypes, OIDOID);
	argtypes = lappend_oid(argtypes, BYTEAOID);
	argtypes = lappend_oid(argtypes, ANYELEMENTOID);
	aggref = makeNode(Aggref);
	aggref->aggfnoid = finalfnoid;
	aggref->aggtype = inp->aggtype; // TODO finalrettype;
	aggref->aggcollid = inp->aggcollid;
	aggref->inputcollid = inp->inputcollid;
	aggref->aggtranstype = InvalidOid; // will be set by planner
	aggref->aggargtypes = argtypes;
	aggref->aggdirectargs = NULL; // TODO
	aggref->aggorder = NULL;
	aggref->aggdistinct = NULL;
	aggref->aggfilter = NULL;
	aggref->aggstar = false;
	aggref->aggvariadic = false;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggsplit = AGGSPLIT_SIMPLE; // TODO make sure plannerdoes not change this ???
	aggref->location = -1;				// unknown
										/* construct the arguments */
	oidtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(OIDOID));
	if (!HeapTupleIsValid(oidtup))
		elog(ERROR, "cache lookup failed for type OIDOID");
	oidform = (Form_pg_type) GETSTRUCT(oidtup);

	oid1arg = makeConst(OIDOID,
						-1,
						InvalidOid,
						oidform->typlen,
						ObjectIdGetDatum(inp->aggfnoid),
						false,
						true // passbyval TODO
	);
	te = makeTargetEntry((Expr *) oid1arg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	// TODO should resno be false???
	oid2arg = makeConst(OIDOID,
						-1,
						InvalidOid,
						oidform->typlen,
						ObjectIdGetDatum(inp->inputcollid),
						false,
						true // passbyval TODO
	);
	te = makeTargetEntry((Expr *) oid2arg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	bytearg = copyObject(inpcol);
	te = makeTargetEntry((Expr *) bytearg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	// TODO what is the collation id here?
	nullarg = makeNullConst(aggref->aggtype, -1, inp->aggcollid);
	te = makeTargetEntry((Expr *) nullarg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	Assert(tlist_attno == 5);
	aggref->args = tlist;
	return aggref;
#undef FINALFN
}
/*
Query * finalizeSelectQuery( Query * orig, List *attrList)
{
	TargetEntry *te;
	ListCell *t;
	Query * newq = copyObject(orig);
	foreach (t, orig->targetList)
	{
  result = makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);

	}
}
*/
static FuncExpr *
get_partialize_func_info(Aggref *agg)
{
	FuncExpr *partialize_fnexpr;
	Oid partfnoid, partrettype;
	partrettype = ANYELEMENTOID;
	partfnoid = LookupFuncName(list_make1(makeString("partialize")), 1, &partrettype, false);
	//		ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(partfnoid));
	//		pform = (Form_pg_proc) GETSTRUCT(ftup);

	partialize_fnexpr = makeFuncExpr(partfnoid,
									 partrettype,
									 list_make1(agg), /*args*/
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);
	return partialize_fnexpr;
}

typedef struct AggPartCxt
{
	List *matcollist; // for mattbl
	List *seltlist;   // TODO remove for fnexpr with original varnos
	List *origcolmap;
	bool addcol;
} AggPartCxt;

static Node *
add_aggregate_partialize_mutator(Node *node, AggPartCxt *cxt)
{
	if (node == NULL)
		return NULL;
	/* modify the aggref and create a partialize(aggref) expr
	 * for the materialization.
	 * replace the aggref with the ts_internal_cagg_final fn.
	 * All new Vars have varno = 1 (for RTE 1)
	 */
	if (IsA(node, Aggref))
	{
#define BUFLEN 10
		char buf[BUFLEN];
		char *colname;
		Aggref *newagg;
		Var *var;
		ColumnDef *col;
		int matcolno = list_length(cxt->matcollist) + 1;

		/* step 1: create partialize( aggref) column for materialization */
		FuncExpr *fexpr = get_partialize_func_info((Aggref *) node);
		snprintf(buf, BUFLEN, "tscol%d", matcolno);
		colname = buf;
		col = makeColumnDef(colname, BYTEAOID, -1, InvalidOid);
		cxt->matcollist = lappend(cxt->matcollist, col);
		cxt->addcol = true;
		/* step 2: create expr for call to
		 * ts_internal_cagg_final( oid, oid, matcol, null)
		 */
		//	varattnos start at 1.
		var = makeVar(1,
					  matcolno,
					  exprType((Node *) fexpr),
					  exprTypmod((Node *) fexpr),
					  exprCollation((Node *) fexpr),
					  0);
		newagg = create_finalize_agg((Aggref *) node, var);
		return (Node *) newagg;
#undef BUFLEN
	}
	return expression_tree_mutator(node, add_aggregate_partialize_mutator, cxt);
}
/*
static Node *
add_aggregate_finalize_mutator(Node *node, bool *cxt)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
	{
		Aggref * = copyObject(get_finalize_func_info((Aggref*)node));
		expr->args = list_make1(node);
		*cxt = true;
		return (Node *) expr;
	}
	return expression_tree_mutator(node, add_aggregate_finalize_mutator, cxt);
}
*/

/* Create a hypertable for the continuous agg materialization.
 *
 * This code is similar to the create_ctas_nodata path.
 * We need custom code because we define additional columns besides
 * what is defined in the query->targetList.
 */
void
cagg_create_mattbl(CreateTableAsStmt *stmt)
{
#define BUFLEN 100
	ObjectAddress address;
	ObjectAddress mataddress;
	Datum toast_options;
	List *attrList = NIL;
	ListCell *t;
	int colcnt = 0;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	char buf[BUFLEN];
	CreateStmt *create;
	Query *view_query;
	Query *orig_query;
	AggPartCxt cxt;
	List *origcolmap;
	int resno = 1;
	/* Modify the INTO clause reloptions
	 * 1. remove timesacle specific options.
	 * 2. modify target list
	 *into->viewQuery is the parsetree which is stored in the
	 * pg_rewrite table and is not modified.*/

	/* make additional columns */
	{
		Node *vexpr = (Node *) makeVar(0, colcnt + 1, INT4OID, -1, InvalidOid, 0);
		ColumnDef *col =
			makeColumnDef("chunk_id", exprType(vexpr), exprTypmod(vexpr), exprCollation(vexpr));
		attrList = lappend(attrList, col);
		vexpr = (Node *) makeVar(0, colcnt + 2, TIMESTAMPTZOID, -1, InvalidOid, 0);
		col = makeColumnDef("mints", exprType(vexpr), exprTypmod(vexpr), exprCollation(vexpr));
		attrList = lappend(attrList, col);
		vexpr = (Node *) makeVar(0, colcnt + 3, TIMESTAMPTZOID, -1, InvalidOid, 0);
		col = makeColumnDef("maxts", exprType(vexpr), exprTypmod(vexpr), exprCollation(vexpr));
		attrList = lappend(attrList, col);
		origcolmap = list_make3_int(0, 0, 0);
	}
	/* We want all the entries in the targetlist (resjunk or not)
	 * in the table defintion so we include group-by/having clause etc.
	 * TODO are the tlist entries distinct?
	 */
	orig_query = (Query *) copyObject(stmt->query);
	cxt.matcollist = attrList;
	cxt.seltlist = NIL;
	cxt.origcolmap = origcolmap;
	// TODO sum(c) + sum(d) should become
	// partialize(sum(c) ), partialize(sum(d))
	foreach (t, ((Query *) stmt->query)->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(t);
		TargetEntry *modte = copyObject(tle);
		cxt.addcol = false;
		modte = (TargetEntry *) expression_tree_mutator((Node *) tle,
														add_aggregate_partialize_mutator,
														&cxt);
		/* We need columns for non-aggregate targets
		 * if it is not a resjunk OR appears in the grouping clause
		 */
		if (cxt.addcol == false && (tle->resjunk == false || tle->ressortgroupref > 0))
		{
			ColumnDef *col;
			char *colname;
			Var *var;
			int matcolno = list_length(cxt.matcollist) + 1;
			if (tle->resname)
				colname = pstrdup(tle->resname);
			else
			{
				snprintf(buf, BUFLEN, "tscol%d", matcolno);
				colname = buf;
			}
			col = makeColumnDef(colname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));
			cxt.matcollist = lappend(cxt.matcollist, col);
			var = makeVar(1,
						  matcolno,
						  exprType((Node *) tle->expr),
						  exprTypmod((Node *) tle->expr),
						  exprCollation((Node *) tle->expr),
						  0);
			/* fix the expression for the target entry */
			modte->expr = (Expr *) var;
		}
		// still need to take care of having, group by clause
		// and deal with duplicate entries here
		/* Below we construct the targetlist for the query on the
		 * materialization table. The TL maps 1-1 with the original
		 * query:
		 * e.g select a, min(b)+max(d) from foo group by a,timebucket(a);
		 * becomes
		 * select <a-col>,
		 * ts_internal_cagg_final(..b-col ) + ts_internal_cagg_final(..d-col)
		 * from mattbl
		 * group by a-col, timebucket(a-col)
		 */
		{
			/*we copy the target entries , resnos should be the same
			 * tleSortGroupRef can be reused, only table info
			 * needs to be modified
			 */
			Assert(modte->resno == resno);
			resno++;
			// push this to later and add resorigtbl info
			// and adjust resnames to the correct one as well.
			if (IsA(modte->expr, Var))
			{
				modte->resorigcol = ((Var *) modte->expr)->varattno;
			}
			cxt.seltlist = lappend(cxt.seltlist, modte);
		}
	}
	/* create a dummy MATVIEW */
	{
		ListCell *lc = list_head(stmt->into->colNames);
		ColumnDef *col;
		List *attrlist = NIL;
		foreach (t, orig_query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(t);
			ColumnDef *col;
			char *colname;
			if (!tle->resjunk && lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(lc);
			}
			else
			{
				snprintf(buf, BUFLEN, "tscol%d", colcnt);
				colname = buf;
			}
			if (!tle->resjunk)
			{
				col = makeColumnDef(colname,
									exprType((Node *) tle->expr),
									exprTypmod((Node *) tle->expr),
									exprCollation((Node *) tle->expr));
				attrlist = lappend(attrlist, col);
			}
		}
		if (lc != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("too many column names were specified")));
		stmt->into->options = NULL;
		create = makeNode(CreateStmt);
		create->relation = stmt->into->rel;
		create->tableElts = attrlist;
		create->inhRelations = NIL;
		create->ofTypename = NULL;
		create->constraints = NIL;
		create->options = stmt->into->options;
		create->oncommit = stmt->into->onCommit;
		create->tablespacename = stmt->into->tableSpaceName;
		create->if_not_exists = false; /* TODO */
		mataddress = DefineRelation(create, RELKIND_MATVIEW, InvalidOid, NULL, NULL);
	}

	/*
	 * Create the hypertable root by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 * Remove the options on the into clause that we will not honour
	 * clause. Modify the relname as well
	 */
	// TODO should the mat table be in the same schema??
	{
		RangeVar *old_rel = stmt->into->rel;
		RangeVar *new_rel = copyObject(stmt->into->rel);
		snprintf(buf, BUFLEN, "ts_internal_%s", old_rel->relname);
		new_rel->relname = buf;
		stmt->into->rel = new_rel;

		stmt->into->options = NULL;
		create = makeNode(CreateStmt);
		create->relation = stmt->into->rel;
		create->tableElts = cxt.matcollist;
		create->inhRelations = NIL;
		create->ofTypename = NULL;
		create->constraints = NIL;
		create->options = stmt->into->options;
		create->oncommit = stmt->into->onCommit;
		create->tablespacename = stmt->into->tableSpaceName;
		create->if_not_exists = false; /* TODO */

		/*  Create the materialization table.
		 *  (This will error out if one already exists
		 *  TODO : do we need to pass ownerid here?
		 */
		// address = DefineRelation(create, RELKIND_MATVIEW, InvalidOid, NULL, NULL);
		address = DefineRelation(create, RELKIND_RELATION, InvalidOid, NULL, NULL);
		CommandCounterIncrement();
	}
	/* now lets construct the select query for the materialization table
	 */
	{
		Query *selquery = copyObject(orig_query);
		// we have only 1 entry in rtable -checked during query validation
		// modify this to reflect the materialization table
		// we just created.
		RangeTblEntry *rte = list_nth(selquery->rtable, 0);
		FromExpr *fromexpr;
		rte->relid = address.objectId;
		rte->rtekind = RTE_RELATION;
		rte->relkind = RELKIND_RELATION;
		rte->tablesample = NULL;
		// aliases for column names
		rte->eref->colnames = NIL;
		foreach (t, cxt.matcollist)
		{
			ColumnDef *cdef = (ColumnDef *) lfirst(t);
			Value *attrname = makeString(cdef->colname);
			rte->eref->colnames = lappend(rte->eref->colnames, attrname);
		}
		rte->insertedCols = NULL;
		rte->updatedCols = NULL;
		/* mark the whole row as needing select privileges */
		/*{
				Var *result = makeWholeRowVar(rte, 1, 0, true);
				result->location = 0;
				markVarForSelectPriv(NULL, result, rte);
			}
		*/
		/* 2. Fixup targetlist with the correct rel information */
		foreach (t, cxt.seltlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(t);
			if (IsA(tle->expr, Var))
			{
				tle->resorigtbl = rte->relid;
				tle->resorigcol = ((Var *) tle->expr)->varattno;
			}
		}
		// fixu correct resname as well
		Assert(stmt->into->colNames); // TODO check this in query validation
		if (stmt->into->colNames != NIL)
		{
			ListCell *alist_item = list_head(stmt->into->colNames);

			foreach (t, cxt.seltlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(t);

				/* junk columns don't get aliases */
				if (tle->resjunk)
					continue;
				tle->resname = pstrdup(strVal(lfirst(alist_item)));
				alist_item = lnext(alist_item);
				if (alist_item == NULL)
					break; /* done assigning aliases */
			}

			if (alist_item != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("too many column names were specified")));
		}
		selquery->targetList = cxt.seltlist;
		/* fixup from list. no quals on original table should be
		 * present here - is this assumption correct TODO ???
		 */
		Assert(list_length(selquery->jointree->fromlist) == 1);
		fromexpr = selquery->jointree;
		fromexpr->quals = NULL;
		/* need to check groupby clause, what if we have a aggref here
		 * also having clause
		 */
		//		StoreViewQuery(address.objectId, selquery, false);
		StoreViewQuery(mataddress.objectId, selquery, false);
		CommandCounterIncrement(); //??TODO???
		return;
	}
	// modquery = finalizeSelectQuery(  orig_query , attrList);

	return;

	// TODO

	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(address.objectId, toast_options);
	view_query = (Query *) copyObject(stmt->into->viewQuery);

	/* TODO when do we need a TOAST table ??? */

	/* TODO stmt->into->skipData to fill/not fill the data  */
#undef BUFLEN
}
