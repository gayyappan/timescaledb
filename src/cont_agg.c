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
#include <parser/parse_relation.h> /*markVarForSelectPriv */
#include <utils/builtins.h>

typedef struct AggPartCxt
{
	List *matcollist;		/* column defns for materialization tbl*/
	List *final_seltlist;   /* tlist entries for SELECT from materialization*/
	List *partial_seltlist; /* tlist entries for populating mat table */
	List *origcolmap;
	bool addcol;
} AggPartCxt;

static void
create_catalog_entry_mattbl(Oid mattbloid, char *matschema_name, char *mattbl_name,
							Query *partial_query)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	NameData schname, tblname;
	Datum values[Natts_cquery_detail];
	bool nulls[Natts_cquery_detail] = { false };
	CatalogSecurityContext sec_ctx;
	char *querystr = nodeToString(partial_query);

	namestrcpy(&schname, matschema_name);
	namestrcpy(&tblname, mattbl_name);
	rel = heap_open(catalog_get_table_id(catalog, CQUERY_DETAIL), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	memset(values, 0, sizeof(values));
	// values[AttrNumberGetAttrOffset(Anum_cquery_hypertable_id)] =
	//                Int32GetDatum(chunk->fd.hypertable_id);
	// TODO replace oid with hypertable_id
	values[AttrNumberGetAttrOffset(Anum_tbloid)] = ObjectIdGetDatum(mattbloid);
	values[AttrNumberGetAttrOffset(Anum_cquery_schema_name)] = NameGetDatum(&schname);
	values[AttrNumberGetAttrOffset(Anum_cquery_table_name)] = NameGetDatum(&tblname);
	values[AttrNumberGetAttrOffset(Anum_cquery_partial_query)] = CStringGetTextDatum(querystr);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
	heap_close(rel, RowExclusiveLock);
}

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
 * This function returns an aggref
 * ts_internal_cagg_final( Oid, Oid, bytea, NULL::output_typeid)
 * the arguments are a list of targetentry
 */
static Aggref *
create_finalize_agg(Aggref *inp, Var *inpcol)
{
#define FINALFN "ts_internal_cagg_final"
	Aggref *aggref;
	TargetEntry *te;
	Const *oid1arg, *oid2arg, *nullarg;
	RelabelType *oid1arg_conv, *oid2arg_conv;
	Var *bytearg;
	List *tlist = NIL;
	int tlist_attno = 1;
	List *argtypes = NIL;
	Oid finalfnargtypes[] = { OIDOID, OIDOID, BYTEAOID, ANYELEMENTOID };
	Oid finalfnoid;
	List *funcname = list_make1(makeString(FINALFN));
	finalfnoid = LookupFuncName(funcname, 4, finalfnargtypes, false);
	/* The arguments are input aggref oid,
	 * inputcollid, bytea column-value, NULL::returntype
	 */
	argtypes = lappend_oid(argtypes, OIDOID);
	argtypes = lappend_oid(argtypes, OIDOID);
	argtypes = lappend_oid(argtypes, BYTEAOID);
	argtypes = lappend_oid(argtypes, inp->aggtype);
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
	oid1arg = makeConst(INT4OID,
						-1,
						InvalidOid,
						sizeof(int32),
						//	oidform->typlen,
						ObjectIdGetDatum(inp->aggfnoid),
						false,
						true // passbyval TODO
	);
	oid1arg_conv = makeRelabelType((Expr *) oid1arg, OIDOID, -1, InvalidOid, COERCE_IMPLICIT_CAST);

	te = makeTargetEntry((Expr *) oid1arg_conv, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	// TODO should resno be false???
	oid2arg = makeConst(INT4OID,
						-1,
						InvalidOid,
						sizeof(int32),
						// oidform->typlen,
						ObjectIdGetDatum(inp->inputcollid),
						false,
						true // passbyval TODO
	);
	oid2arg_conv = makeRelabelType((Expr *) oid2arg, OIDOID, -1, InvalidOid, COERCE_IMPLICIT_CAST);
	te = makeTargetEntry((Expr *) oid2arg_conv, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	bytearg = copyObject(inpcol);
	te = makeTargetEntry((Expr *) bytearg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	// TODO what is the collation id here?
	nullarg = makeNullConst(inp->aggtype, -1, inp->aggcollid);
	te = makeTargetEntry((Expr *) nullarg, tlist_attno++, NULL, true);
	tlist = lappend(tlist, te);
	Assert(tlist_attno == 5);
	aggref->args = tlist;
	return aggref;
#undef FINALFN
}

static FuncExpr *
get_partialize_func_info(Aggref *agg)
{
	FuncExpr *partialize_fnexpr;
	Oid partfnoid, partrettype;
	partrettype = ANYELEMENTOID;
	partfnoid = LookupFuncName(list_make1(makeString("partialize")), 1, &partrettype, false);
	partialize_fnexpr = makeFuncExpr(partfnoid,
									 partrettype,
									 list_make1(agg), /*args*/
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);
	return partialize_fnexpr;
}

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
		cxt->partial_seltlist = lappend(cxt->partial_seltlist, fexpr);
		/* step 2: create expr for call to
		 * ts_internal_cagg_final( oid, oid, matcol, null)
		 */
		/* This is a var for the column we created */
		var = makeVar(1, matcolno, BYTEAOID, -1, InvalidOid, 0);
		newagg = create_finalize_agg((Aggref *) node, var);
		return (Node *) newagg;
#undef BUFLEN
	}
	return expression_tree_mutator(node, add_aggregate_partialize_mutator, cxt);
}

/* Make a select query for the materialization table
 * origquery - query passed to create view
 * mattbladdress - materialization table ObjectAddress
 * tlist_aliases - aliases for targetlist entries
 */
static Query *
createSelQueryForMatTbl(Query *origquery, AggPartCxt *cxt, ObjectAddress *mattbladdress,
						List *tlist_aliases)
{
	// char *selstr = NULL;
	ListCell *lc;
	Query *final_selquery = copyObject(origquery);
	/* we have only 1 entry in rtable -checked during query validation
	 * modify this to reflect the materialization table we just
	 * created.
	 */
	RangeTblEntry *rte = list_nth(final_selquery->rtable, 0);
	FromExpr *fromexpr;
	Var *result;
	rte->relid = mattbladdress->objectId;
	rte->rtekind = RTE_RELATION;
	rte->relkind = RELKIND_RELATION;
	rte->tablesample = NULL;
	rte->eref->colnames = NIL;
	// aliases for column names for the materialization table
	foreach (lc, cxt->matcollist)
	{
		ColumnDef *cdef = (ColumnDef *) lfirst(lc);
		Value *attrname = makeString(cdef->colname);
		rte->eref->colnames = lappend(rte->eref->colnames, attrname);
	}
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	result = makeWholeRowVar(rte, 1, 0, true);
	result->location = 0;
	markVarForSelectPriv(NULL, result, rte);
	/* 2. Fixup targetlist with the correct rel information */
	foreach (lc, cxt->final_seltlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (IsA(tle->expr, Var))
		{
			tle->resorigtbl = rte->relid;
			tle->resorigcol = ((Var *) tle->expr)->varattno;
		}
	}
	// fixu correct resname as well
	if (tlist_aliases != NIL)
	{
		ListCell *alist_item = list_head(tlist_aliases);
		foreach (lc, cxt->final_seltlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

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
	final_selquery->targetList = cxt->final_seltlist;
	/* fixup from list. no quals on original table should be
	 * present here - is this assumption correct TODO ???
	 */
	Assert(list_length(final_selquery->jointree->fromlist) == 1);
	fromexpr = final_selquery->jointree;
	fromexpr->quals = NULL;
	/* TODO need to check groupby clause, what if we have a aggref here
	 * also having clause
	 */
	// selstr = nodeToString(final_selquery);
	return final_selquery;
}

/* Modifies the passed in ViewStmt to do the following
 * a) Create a hypertable for the continuous agg materialization.
 * b) create a view that references the underlying
 * materialization table instead of the original table used in
 * the CREATE VIEW stmt.
 * Example:
 * CREATE VIEW mcagg ...
 * AS  select a, min(b)+max(d) from foo group by a,timebucket(a);
 * Step1. create a materialiation table which stores the partials for the
 * aggregates and the grouping columns + internal columns.
 * So we have a table like ts_internal_mcagg
 * with columns:
 *( internal-columns , a, partialize(min(b)), partialize(max(d)), timebucket(a))
 * Step2: Create a view with modified select query
 * CREATE VIEW mcagg
 * as
 * select a, finalize( partialize(min(b)) + finalize(partialize(max(d))
 * from ts_internal_mcagg
 * group by a, timebucket(a)
 *
 * Notes: ViewStmt->query is the raw parse tree
 * panquery is the output of runningparse_anlayze( ViewStmt->query)
 */
void
cagg_create(ViewStmt *stmt, Query *panquery)
{
#define BUFLEN 100
	ObjectAddress address, mataddress;
	// Datum toast_options;
	List *attrList = NIL;
	ListCell *lc;
	int colcnt = 0;
	// static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	char buf[BUFLEN];
	Query *final_selquery;
	Query *partial_selquery; /* query to populate the mattable*/
	List *selcollist = NIL;
	CreateStmt *create;
	Query *origquery;
	AggPartCxt cxt;
	List *origcolmap;
	RangeVar *mat_rel = NULL;
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
	origquery = (Query *) copyObject(panquery);
	cxt.matcollist = attrList;
	cxt.final_seltlist = NIL;
	cxt.partial_seltlist = NIL;
	cxt.origcolmap = origcolmap;
	foreach (lc, ((Query *) panquery)->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
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
			cxt.partial_seltlist = lappend(cxt.partial_seltlist, copyObject(tle));
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
			// TODO push this to later and add resorigtbl info
			// and adjust resnames to the correct one as well.
			if (IsA(modte->expr, Var))
			{
				modte->resorigcol = ((Var *) modte->expr)->varattno;
			}
			cxt.final_seltlist = lappend(cxt.final_seltlist, modte);
		}
	}

	/*
	 * Create the materialization hypertable root by faking up a
	 * CREATE TABLE parsetree and passing it to DefineRelation.
	 * Remove the options on the into clause that we will not honour
	 * Modify the relname to ts_internal_<name>
	 */
	// TODO should the mat table be in the same schema,
	// TODO convert this to a hypertable??
	{
		RangeVar *old_rel = stmt->view;
		mat_rel = copyObject(old_rel);
		snprintf(buf, BUFLEN, "ts_internal_%s", old_rel->relname);
		mat_rel->relname = buf;
		stmt->options = NULL;

		create = makeNode(CreateStmt);
		create->relation = mat_rel;
		create->tableElts = cxt.matcollist;
		create->inhRelations = NIL;
		create->ofTypename = NULL;
		create->constraints = NIL;
		create->options = NULL;
		create->oncommit = ONCOMMIT_NOOP;
		create->tablespacename = NULL; // TODO what is the default?
		create->if_not_exists = false;

		/*  Create the materialization table.
		 *  TODO : do we need to pass ownerid here?
		 */
		mataddress = DefineRelation(create, RELKIND_RELATION, InvalidOid, NULL, NULL);
		CommandCounterIncrement();
	}
	/* construct the select query for the materialization table
	 * created above and create the view
	 */
	final_selquery = createSelQueryForMatTbl(origquery, &cxt, &mataddress, stmt->aliases);
	foreach (lc, final_selquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (!tle->resjunk)
		{
			ColumnDef *col = makeColumnDef(tle->resname,
										   exprType((Node *) tle->expr),
										   exprTypmod((Node *) tle->expr),
										   exprCollation((Node *) tle->expr));
			selcollist = lappend(selcollist, col);
		}
	}
	create = makeNode(CreateStmt);
	create->relation = stmt->view;
	create->tableElts = selcollist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = NULL; // TODO what is the default?
	create->if_not_exists = false;

	/*  Create the materialization table.
	 *  TODO : do we need to pass ownerid here?
	 */
	address = DefineRelation(create, RELKIND_VIEW, InvalidOid, NULL, NULL);
	CommandCounterIncrement();
	StoreViewQuery(address.objectId, final_selquery, false);
	CommandCounterIncrement();

	/* create the query with select partialize(..)
	 *remove HAVING clause and TODO --- ORDER BY
	 */
	partial_selquery = copyObject(panquery);
	partial_selquery->targetList = cxt.partial_seltlist;
	partial_selquery->havingQual = NULL;

	Assert(mat_rel != NULL);
	//TODO need to fill in correct schema name here, mat_rel->schemaname could be invalid.
	create_catalog_entry_mattbl(mataddress.objectId,
								//mat_rel->schemaname,
								"test",
								mat_rel->relname,
								partial_selquery);
	/* record dependency: view depends on materialization table */
	// TODO any other dependencies??
	recordDependencyOn(&mataddress, &address, DEPENDENCY_INTERNAL);

	return;

	// TODO

	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	// toast_options =
	//		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	//	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	//	NewRelationCreateToastTable(address.objectId, toast_options);

	/* TODO when do we need a TOAST table ??? */

	/* TODO stmt->into->skipData to fill/not fill the data  */
#undef BUFLEN
}
