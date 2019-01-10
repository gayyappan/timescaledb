/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <parser/parsetree.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <catalog/namespace.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/var.h>
#include <optimizer/restrictinfo.h>
#include <utils/lsyscache.h>
#include <executor/nodeAgg.h>
#include <utils/timestamp.h>
#include <utils/selfuncs.h>
#include <access/sysattr.h>
#include "compat-msvc-enter.h"
#include <optimizer/cost.h>
#include <tcop/tcopprot.h>
#include <optimizer/plancat.h>
#include <nodes/nodeFuncs.h>

#include <catalog/pg_constraint.h>
#include "compat.h"
#if PG96 || PG10				/* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#endif
#include "compat-msvc-exit.h"

#include "cross_module_fn.h"
#include "license_guc.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "utils.h"
#include "guc.h"
#include "dimension.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_insert.h"
#include "hypertable_update.h"
#include "constraint_aware_append.h"
#include "partitioning.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "chunk.h"
#include "plan_expand_hypertable.h"
#include "plan_add_hashagg.h"
#include "plan_agg_bookend.h"

void		_planner_init(void);
void		_planner_fini(void);

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;
static PlannedStmt* 
ts_hypertable_process_plannedstmt( PlannedStmt *pstmt);

#define CTE_NAME_HYPERTABLES "hypertable_parent"

static void
mark_rte_hypertable_parent(RangeTblEntry *rte)
{
	/*
	 * CTE name is never used for regular tables so use that as a signal that
	 * the rte is a hypertable.
	 */
	Assert(rte->ctename == NULL);
	rte->ctename = CTE_NAME_HYPERTABLES;
}

static bool
is_rte_hypertable(RangeTblEntry *rte)
{
	return rte->ctename != NULL && strcmp(rte->ctename, CTE_NAME_HYPERTABLES) == 0;
}

/* This turns off inheritance on hypertables where we will do chunk
 * expansion ourselves. This prevents postgres from expanding the inheritance
 * tree itself. We will expand the chunks in timescaledb_get_relation_info_hook. */
static bool
turn_off_inheritance_walker(Node *node, Cache *hc)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		ListCell   *lc;
		int			rti = 1;

		foreach(lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst(lc);

			if (rte->inh)
			{
				Hypertable *ht = ts_hypertable_cache_get_entry(hc, rte->relid);

				if (NULL != ht && ts_plan_expand_hypertable_valid_hypertable(ht, query, rti, rte))
				{
					rte->inh = false;
					mark_rte_hypertable_parent(rte);
				}
			}
			rti++;
		}

		return query_tree_walker(query, turn_off_inheritance_walker, hc, 0);
	}

	return expression_tree_walker(node, turn_off_inheritance_walker, hc);
}

static PlannedStmt *
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
{
	PlannedStmt *stmt;

	if (ts_extension_is_loaded() && !ts_guc_disable_optimizations && parse->resultRelation == 0)
	{
		Cache	   *hc = ts_hypertable_cache_pin();

		/*
		 * turn of inheritance on hypertables we will expand ourselves in
		 * timescaledb_get_relation_info_hook
		 */
		turn_off_inheritance_walker((Node *) parse, hc);

		ts_cache_release(hc);
	}

	if (prev_planner_hook != NULL)
		/* Call any earlier hooks */
		return (prev_planner_hook) (parse, cursor_opts, bound_params);

	/* Call the standard planner */
	stmt = standard_planner(parse, cursor_opts, bound_params);
        stmt = ts_hypertable_process_plannedstmt(stmt);
        //ts_hypertable_process_plannedstmt(stmt);
	   //TO_DO move following to the post planner as well.

	/*
	 * Our top-level HypertableInsert plan node that wraps ModifyTable needs
	 * to have a final target list that is the same as the ModifyTable plan
	 * node, and we only have access to its final target list after
	 * set_plan_references() (setrefs.c) has run at the end of
	 * standard_planner. Therefore, we fixup the final target list for
	 * HypertableInsert here.
	 */
	stmt->planTree = ts_hypertable_insert_fixup_tlist(stmt->planTree);

	return stmt;
}


static inline bool
should_optimize_query(Hypertable *ht)
{
	return !ts_guc_disable_optimizations &&
		(ts_guc_optimize_non_hypertables || ht != NULL);
}


extern void ts_sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

static inline bool
should_optimize_append(const Path *path)
{
	RelOptInfo *rel = path->parent;
	ListCell   *lc;

	if (!ts_guc_constraint_aware_append ||
		constraint_exclusion == CONSTRAINT_EXCLUSION_OFF)
		return false;

	/*
	 * If there are clauses that have mutable functions, this path is ripe for
	 * execution-time optimization.
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (contain_mutable_functions((Node *) rinfo->clause))
			return true;
	}
	return false;
}


static inline bool
is_append_child(RelOptInfo *rel, RangeTblEntry *rte)
{
	return rel->reloptkind == RELOPT_OTHER_MEMBER_REL &&
		rte->inh == false &&
		rel->rtekind == RTE_RELATION &&
		rte->relkind == RELKIND_RELATION;
}

static inline bool
is_append_parent(RelOptInfo *rel, RangeTblEntry *rte)
{
	return rel->reloptkind == RELOPT_BASEREL &&
		rte->inh == true &&
		rel->rtekind == RTE_RELATION &&
		rte->relkind == RELKIND_RELATION;
}

static void
timescaledb_set_rel_pathlist(PlannerInfo *root,
							 RelOptInfo *rel,
							 Index rti,
							 RangeTblEntry *rte)
{
	Hypertable *ht;
	Cache	   *hcache;

	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook) (root, rel, rti, rte);

	if (!ts_extension_is_loaded() || IS_DUMMY_REL(rel) || !OidIsValid(rte->relid))
		return;

	/* quick abort if only optimizing hypertables */
	if (!ts_guc_optimize_non_hypertables && !(is_append_parent(rel, rte) || is_append_child(rel, rte)))
		return;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

	if (!should_optimize_query(ht))
		goto out_release;

	if (ts_guc_optimize_non_hypertables)
	{
		/* if optimizing all tables, apply optimization to any table */
		ts_sort_transform_optimization(root, rel);
	}
	else if (ht != NULL && is_append_child(rel, rte))
	{
		/* Otherwise, apply only to hypertables */

		/*
		 * When applying to hypertables, apply when you get the first append
		 * relation child (indicated by RELOPT_OTHER_MEMBER_REL) which is the
		 * main table. Then apply to all other children of that hypertable. We
		 * can't wait to get the parent of the append relation b/c by that
		 * time it's too late.
		 */
		ListCell   *l;

		foreach(l, root->append_rel_list)
		{
			AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
			RelOptInfo *siblingrel;

			/*
			 * Note: check against the reloid not the index in the
			 * simple_rel_array since the current rel is not the parent but
			 * just the child of the append_rel representing the main table.
			 */
			if (appinfo->parent_reloid != rte->relid)
				continue;
			siblingrel = root->simple_rel_array[appinfo->child_relid];
			ts_sort_transform_optimization(root, siblingrel);
		}
	}

	if (

	/*
	 * Right now this optimization applies only to hypertables (ht used
	 * below). Can be relaxed later to apply to reg tables but needs testing
	 */
		ht != NULL &&
		is_append_parent(rel, rte) &&
	/* Do not optimize result relations (INSERT, UPDATE, DELETE) */
		0 == root->parse->resultRelation)
	{
		ListCell   *lc;

		foreach(lc, rel->pathlist)
		{
			Path	  **pathptr = (Path **) &lfirst(lc);
			Path	   *path = *pathptr;

			switch (nodeTag(path))
			{
				case T_AppendPath:
				case T_MergeAppendPath:
					if (should_optimize_append(path))
						*pathptr = ts_constraint_aware_append_path_create(root, ht, path);
				default:
					break;
			}
		}
	}

out_release:
	ts_cache_release(hcache);
}

/* This hook is meant to editorialize about the information
 * the planner gets about a relation. We hijack it here
 * to also expand the append relation for hypertables. */
static void
timescaledb_get_relation_info_hook(PlannerInfo *root,
								   Oid relation_objectid,
								   bool inhparent,
								   RelOptInfo *rel)
{
	RangeTblEntry *rte;

	if (prev_get_relation_info_hook != NULL)
		prev_get_relation_info_hook(root, relation_objectid, inhparent, rel);

	if (!ts_extension_is_loaded())
		return;

	rte = rt_fetch(rel->relid, root->parse->rtable);

	/*
	 * We expand the hypertable chunks into an append relation. Previously, in
	 * `turn_off_inheritance_walker` we suppressed this expansion. This hook
	 * is really the first one that's called after the initial planner setup
	 * and so it's convenient to do the expansion here. Note that this is
	 * after the usual expansion happens in `expand_inherited_tables` (called
	 * in `subquery_planner`). Note also that `get_relation_info` (the
	 * function that calls this hook at the end) is the expensive function to
	 * run on many chunks so the expansion really cannot be called before this
	 * hook.
	 */
	if (!rte->inh && is_rte_hypertable(rte))
	{

		Cache	   *hcache = ts_hypertable_cache_pin();
		Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rte->relid);

		Assert(ht != NULL);

		ts_plan_expand_hypertable_chunks(ht,
										 root,
										 relation_objectid,
										 inhparent,
										 rel);
#if !PG96 && !PG10
		setup_append_rel_array(root);
#endif

		ts_cache_release(hcache);
	}
}

static bool
involves_ts_hypertable_relid(PlannerInfo *root, Index relid)
{
	if (relid == 0)
		return false;

	return is_rte_hypertable(planner_rt_fetch(relid, root));
}

static bool
involves_hypertable_relid_set(PlannerInfo *root, Relids relid_set)
{
	int			relid = -1;

	while ((relid = bms_next_member(relid_set, relid)) >= 0)
	{
		if (involves_ts_hypertable_relid(root, relid))
			return true;
	}
	return false;
}

static bool
involves_hypertable(PlannerInfo *root, RelOptInfo *rel)
{
	RangeTblEntry *rte;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
			/* Optimization for a quick exit */
			rte = planner_rt_fetch(rel->relid, root);
			if (!(is_append_parent(rel, rte) || is_append_child(rel, rte)))
				return false;

			return involves_ts_hypertable_relid(root, rel->relid);
		case RELOPT_JOINREL:
			return involves_hypertable_relid_set(root,
												 rel->relids);
		default:
			return false;
	}
}


/*
 * Replace INSERT (ModifyTablePath) paths on hypertables.
 *
 * From the ModifyTable description: "Each ModifyTable node contains
 * a list of one or more subplans, much like an Append node.  There
 * is one subplan per result relation."
 *
 * The subplans produce the tuples for INSERT, while the result relation is the
 * table we'd like to insert into.
 *
 * The way we redirect tuples to chunks is to insert an intermediate "chunk
 * dispatch" plan node, between the ModifyTable and its subplan that produces
 * the tuples. When the ModifyTable plan is executed, it tries to read a tuple
 * from the intermediate chunk dispatch plan instead of the original
 * subplan. The chunk plan reads the tuple from the original subplan, looks up
 * the chunk, sets the executor's resultRelation to the chunk table and finally
 * returns the tuple to the ModifyTable node.
 *
 * We also need to wrap the ModifyTable plan node with a HypertableInsert node
 * to give the ChunkDispatchState node access to the ModifyTableState node in
 * the execution phase.
 *
 * Conceptually, the plan modification looks like this:
 *
 * Original plan:
 *
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 *
 * Modified plan:
 *
 *	[ HypertableInsert ]
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^			   ^
 *		  | Tuple	  / <Set resultRelation to the matching chunk table>
 *		  |			 /
 * [ ChunkDispatch ]
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 * PG11 adds a value to the create_upper_paths_hook for FDW support. (See:
 * https://github.com/postgres/postgres/commit/7e0d64c7a57e28fbcf093b6da9310a38367c1d75).
 * Additionally, it calls the hook in a different place, once for each
 * RelOptInfo (see:
 * https://github.com/postgres/postgres/commit/c596fadbfe20ff50a8e5f4bc4b4ff5b7c302ecc0),
 * we do not change our behavior yet, but might choose to in the future.
 */
static List *
replace_hypertable_insert_paths(PlannerInfo *root, List *pathlist)
{
	Cache	   *htcache = ts_hypertable_cache_pin();
	List	   *new_pathlist = NIL;
	ListCell   *lc;

	foreach(lc, pathlist)
	{
		Path	   *path = lfirst(lc);

		if (IsA(path, ModifyTablePath) &&
			((ModifyTablePath *) path)->operation == CMD_INSERT)
		{
			ModifyTablePath *mt = (ModifyTablePath *) path;
			RangeTblEntry *rte = planner_rt_fetch(linitial_int(mt->resultRelations), root);
			Hypertable *ht = ts_hypertable_cache_get_entry(htcache, rte->relid);

			if (NULL != ht)
				path = ts_hypertable_insert_path_create(root, mt);
		}

		new_pathlist = lappend(new_pathlist, path);
	}

	ts_cache_release(htcache);

	return new_pathlist;
}
        
static 
bool ts_has_part_attrs( Hypertable *ht, Bitmapset *collist)
{
  int i;
  Hyperspace *hs = ht->space;
  for(i=0; i < hs->num_dimensions; i++)
  {
     AttrNumber dimattno = hs->dimensions[i].column_attno;
     if (bms_is_member(dimattno - FirstLowInvalidHeapAttributeNumber,
			     collist))
         return true;
  }
  return false;
}
static PlannedStmt* 
ts_hypertable_process_plannedstmt( PlannedStmt *pstmt)
{
    Plan       *plan = pstmt->planTree;

    /*for updates we add a custom scan node here
    * We cannot do this like the INSERT path because inheritance_planner->
    * grouping_planner to create subplans for each inherited table and when
    * that is done, it cfreates a ModifyTable node.
    * create_upper_rel_hook is called from grouping_planner. And we don't have
    * a corresponding ModifyTable path at that point.
    */
    if( IsA(plan, ModifyTable) )
    {
        ModifyTable *mtplan = (ModifyTable*)plan;
        if (mtplan->operation == CMD_UPDATE)
        {
               List       *resultRelations = pstmt->resultRelations;
               List       *rangeTable = pstmt->rtable;
               Cache      *hcache = ts_hypertable_cache_pin();
               Hypertable *ht;
	       ListCell   *l;
               bool partColUpd = FALSE;
               /* safe to assume all operations involve hypertable parts and exit
                 prematurely ???
               */
               foreach(l, resultRelations)
               {
                        Index           relIndex = lfirst_int(l);
                        Oid                     relOid;

                        relOid = getrelid(relIndex, rangeTable);
                        ht = ts_hypertable_cache_get_entry(hcache, relOid);
                        if( ht != NULL )
                        {
                           /*we have a hypertable. is the partition 
			    * column used in set clause of UPDATE.
			    * then we can have potential partition movement of a row */
	                   RangeTblEntry *htrte ;
			   htrte = rt_fetch(relIndex, rangeTable);
                           partColUpd = ts_has_part_attrs(ht, htrte->updatedCols);
                           break;
                        }
                }
                ts_cache_release(hcache);
                if( partColUpd ) /* modify the planned stmt tree */	
                { 
		   Plan * pl = ts_hypertable_update_plan_create( mtplan );
		   pstmt->planTree = pl;
		}
		    
        }
    }
    return pstmt;    
}


static void
#if PG96 || PG10
timescale_create_upper_paths_hook(PlannerInfo *root,
								  UpperRelationKind stage,
								  RelOptInfo *input_rel,
								  RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel);
#else
timescale_create_upper_paths_hook(PlannerInfo *root,
								  UpperRelationKind stage,
								  RelOptInfo *input_rel,
								  RelOptInfo *output_rel,
								  void *extra)
{
	Query	   *parse = root->parse;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);
#endif

	if (!ts_extension_is_loaded())
		return;

	if (ts_cm_functions->create_upper_paths_hook != NULL)
		ts_cm_functions->create_upper_paths_hook(root, stage, input_rel, output_rel);

	/* Modify for INSERTs on a hypertable */
	if (output_rel != NULL && output_rel->pathlist != NIL)
		output_rel->pathlist = replace_hypertable_insert_paths(root, output_rel->pathlist);

	if (ts_guc_disable_optimizations ||
		input_rel == NULL ||
		IS_DUMMY_REL(input_rel))
		return;

	if (!ts_guc_optimize_non_hypertables && !involves_hypertable(root, input_rel))
		return;

	if (UPPERREL_GROUP_AGG == stage)
	{
		ts_plan_add_hashagg(root, input_rel, output_rel);
		if (parse->hasAggs)
			ts_preprocess_first_last_aggregates(root, root->processed_tlist);
	}
}


void
_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = timescaledb_planner;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = timescaledb_set_rel_pathlist;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = timescaledb_get_relation_info_hook;
	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = timescale_create_upper_paths_hook;
}

void
_planner_fini(void)
{
	planner_hook = prev_planner_hook;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	get_relation_info_hook = prev_get_relation_info_hook;
	create_upper_paths_hook = prev_create_upper_paths_hook;
}
