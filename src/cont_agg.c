/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <catalog/pg_type.h>      /* INT4OID */
#include <catalog/toasting.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <access/xact.h>   //CommandCounterIncrement
#include <access/reloptions.h>
#include "compat.h"
#include <hypertable_cache.h>
#include <cont_agg.h>

void cagg_validate_query( Query *query)
{
	Cache	   *hcache;
	Hypertable *ht = NULL;
	RangeTblRef *rtref = NULL;
	RangeTblEntry *rte;
	List *fromList;
	Oid relid;

        if( query->commandType != CMD_SELECT )
	{
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("Only SELECT query permitted for continuous aggs")));
        }
        /* check if we have a hypertable here */
        fromList = query->jointree->fromlist;
	if( list_length(fromList) != 1 )
	{
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("Only 1 hypertable is permitted in SELECT query permitted for continuous aggs")));
	}
        rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
	rte = list_nth( query->rtable, rtref->rtindex - 1);
	relid = rte->relid;
	if( rte->relkind == RELKIND_RELATION )
	{
		hcache = ts_hypertable_cache_pin();
		ht = ts_hypertable_cache_get_entry(hcache, relid);
		ts_cache_release(hcache);
	}
	if( ht == NULL )
	{
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("Can create continuous aggregate only on hypertables")));
	}
        if( query->hasSubLinks || query->hasWindowFuncs ||
		query->hasRecursive || query->hasTargetSRFs )
	{
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("Invalid SELECT query for continuous aggregate")));
	}
        if( !query->hasAggs )
	{
		/*query can ahve aggregate without
		 * group by */
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("SELECT query for continuous aggregateshould have atleast 1 aggregate function ")));
	}
	/* TODO - handle grouping sets and havingClause
	 * DISTINCT
	 */
	if( query->groupingSets)
	{
           ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
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

/* Create a hypertable for the continuous agg materialization.
 *
 * This code is similar to the create_ctas_nodata path. 
 * We need custom code because we define additional columns besides
 * what is defined in the query->targetList.
 */
void cagg_create_mattbl( CreateTableAsStmt *stmt)
{
#define BUFLEN 10
	ObjectAddress address;
	Datum toast_options;
        List       *attrList; 
	ListCell   *t, *lc;
	int colcnt = 0;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	char buf[BUFLEN];
	CreateStmt *create;
	Query *view_query;
	/* Modify the INTO clause reloptions 
	 * 1. remove timesacle specific options.
	 * 2. modify target list 
	 *into->viewQuery is the parsetree which is stored in the
	 * pg_rewrite table and is not modified.*/


	/* We want all the entries in the targetlist (resjunk or not)
	* in the table defintion so we include group-by/having clause etc.
	* TODO are the tlist entries distinct?
	*/

        attrList = NIL;
        lc = list_head(stmt->into->colNames);
        foreach(t, ((Query*)stmt->query)->targetList)
        {
                TargetEntry *tle = (TargetEntry *) lfirst(t);

                ColumnDef  *col;
                char       *colname;
                if (!tle->resjunk && lc )
                {
                        colname = strVal(lfirst(lc));
                        lc = lnext(lc);
		}
                else
		{
			 snprintf(buf, BUFLEN, "tscol%d", colcnt);
                         colname = buf;
		}
                 col = makeColumnDef(colname, exprType((Node *) tle->expr),
				exprTypmod((Node *) tle->expr),
 				exprCollation((Node *) tle->expr));
		 colcnt++;

              /*
               * It's possible that the column is of a collatable type but the
               * collation could not be resolved, so double-check.  (We must
               * check this here because DefineRelation would adopt the type's
               * default collation rather than complaining.)
              */
                 if (!OidIsValid(col->collOid) &&
                               type_is_collatable(col->typeName->typeOid))
                                ereport(ERROR,
                            (errcode(ERRCODE_INDETERMINATE_COLLATION),
      errmsg("no collation was derived for column \"%s\" ", col->colname ),
 errhint("Use the COLLATE clause to set the collation explicitly.")));

               attrList = lappend(attrList, col);
        }

        if (lc != NULL)
                ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("too many column names were specified")));
	/* make additional columns */
        {
        Node * vexpr = (Node *) makeVar(0, colcnt+1, INT4OID, -1, InvalidOid, 0);
       	ColumnDef * col = makeColumnDef("chunk_id", exprType( vexpr),
				exprTypmod( vexpr), exprCollation( vexpr));
                        attrList = lappend(attrList, col);
        vexpr = (Node *) makeVar(0, colcnt+2, TIMESTAMPTZOID, -1, InvalidOid, 0);
       	col = makeColumnDef("mints", exprType( vexpr),
				exprTypmod( vexpr), exprCollation( vexpr));
                        attrList = lappend(attrList, col);
        vexpr = (Node *) makeVar(0, colcnt+3, TIMESTAMPTZOID, -1, InvalidOid, 0);
       	col = makeColumnDef("maxts", exprType( vexpr),
				exprTypmod( vexpr), exprCollation( vexpr));
                        attrList = lappend(attrList, col);
	}

      /*
         * Create the hypertable root by faking up a CREATE TABLE parsetree and
         * passing it to DefineRelation.
	 * Remove the options on the into clause that we will not honour
	 * clause.
         */
        stmt->into->options = NULL;
        create = makeNode(CreateStmt);
        create->relation = stmt->into->rel;
        create->tableElts = attrList;
        create->inhRelations = NIL;
        create->ofTypename = NULL;
        create->constraints = NIL;
        create->options = stmt->into->options;
        create->oncommit = stmt->into->onCommit;
        create->tablespacename = stmt->into->tableSpaceName;
        create->if_not_exists = false;   /* TODO */

       /*  Create the relation.  
	*  (This will error out if one already exists 
	*  TODO : do we need to pass ownerid here?
	*/
        address = DefineRelation(create, RELKIND_MATVIEW, InvalidOid, NULL, NULL);
	CommandCounterIncrement();

	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	toast_options = transformRelOptions((Datum) 0, create->options, 
			"toast", validnsps, true, false);
        (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
        NewRelationCreateToastTable(address.objectId, toast_options);
	view_query = (Query*)copyObject(stmt->into->viewQuery);
	StoreViewQuery( address.objectId, view_query, false);
	CommandCounterIncrement();

	/* TODO when do we need a TOAST table ??? */

	/* TODO stmt->into->skipData to fill/not fill the data  */
}

