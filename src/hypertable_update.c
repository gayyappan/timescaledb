/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <executor/nodeModifyTable.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <foreign/foreign.h>
#include <catalog/pg_type.h>
#include <nodes/execnodes.h>  //ModifyTableState
#include <hypertable_update.h>
#include <chunk_dispatch_plan.h>

static Node * htupdate_plan_state_create(CustomScan *cscan);
static Plan *
htupdate_redo_plan_create( Plan *pl);

/******************** UPDATE SQL stmt for hypertable ********************************/

/*
 * HypertableUpdate (with corresponding executor node) is a plan node that
 * implements UPDATEs for hypertables. It is mostly a wrapper around the
 * ModifyTable plan node that simply calls the wrapped ModifyTable plan 
 * If ModifyTable plan throws an error for partition movement, this node
 * takes over
 *
 */
static void
htupdate_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableUpdateState *state = (HypertableUpdateState *) node;
	PlanState  *ps;
	/* now we save ModifyTableState in custom_ps */
	ps = ExecInitNode(&state->mt->plan, estate, eflags);
	node->custom_ps = list_make1(ps);
	ps = ExecInitNode(&state->del_mt->plan, estate, eflags);
	node->custom_ps = lappend(node->custom_ps, ps);
	ps = ExecInitNode(&state->ins_mt->plan, estate, eflags);
	node->custom_ps = lappend(node->custom_ps, ps);

}

static TupleTableSlot *
htupdate_exec(CustomScanState *node)
{
	TupleTableSlot * tup = NULL;
	MemoryContext ccxt = CurrentMemoryContext;
mretry:  
	PG_TRY();
	{
	   tup = ExecProcNode(linitial(node->custom_ps));
	
	}
	PG_CATCH();
	{
	        ErrorData  *errdata;
	        MemoryContext ecxt;

	        ecxt = MemoryContextSwitchTo(ccxt);
	        errdata = CopyErrorData();
	        if (errdata->sqlerrcode == ERRCODE_CHECK_VIOLATION )
	        {
			/*lets assume it is our part constraint violation
			 * TODO check what violation it is */
	                FlushErrorState();
                        {
HypertableUpdateState *hupstate = (HypertableUpdateState*)node;
ModifyTableState *mtstate = linitial( node->custom_ps);
HypertableUpdateSubplanState *upsubplstate = (HypertableUpdateSubplanState *)(mtstate->mt_plans[mtstate->mt_whichplan]);
        ModifyTableState *delmtstate = lsecond( node->custom_ps);
ModifyTableState *insmtstate = lthird( node->custom_ps);
{
	/* lets do the delete first */
        /* if this does NOT delete any rows then we have a 
	 * potential probelm. Do not attempt to INSERT
	 * Return with a failure from here
	 */
ResultRelInfo *saved_resultrel = NULL;
HypertableUpdateRedoState *delredostate = (HypertableUpdateRedoState *)delmtstate->mt_plans[0];
delredostate->saved_tup = upsubplstate->subplan_tup;
delmtstate->mt_done = 0;
delmtstate->mt_whichplan=0;
delmtstate->fireBSTriggers = false; //disable before stmt triggers
/* the delmtstate  modifytablestate has a DELETE plan for the root
 * hypertable. But we need to delete from the chunk.
 * At the time of constraint error, mtstate is looking
 * at this chunk. So gte the resultRelInfo from mtstate
 * and update delmtstate with it.
 */
//add try-catch block here TODO and restore the pointers back
//before throwing any errors
//use the correct resultRelInfo from mtstate -we need the correct chunks
//resultRelInfo here.

  saved_resultrel = delmtstate->resultRelInfo ;
  delmtstate->resultRelInfo = &mtstate->resultRelInfo[mtstate->mt_whichplan];
  tup = ExecProcNode( (PlanState*)delmtstate);
  delmtstate->resultRelInfo = saved_resultrel;
}

{
	ChunkDispatchState *cdstate = (ChunkDispatchState*)insmtstate->mt_plans[0];
HypertableUpdateRedoState *redostate = (HypertableUpdateRedoState *)linitial(cdstate->cscan_state.custom_ps);
redostate->saved_tup = upsubplstate->subplan_tup;
ts_chunk_dispatch_state_set_parent(cdstate, insmtstate);
/* we have to reset state for insmtstate */
insmtstate->mt_done = 0;
insmtstate->mt_whichplan=0;
insmtstate->fireBSTriggers = false; //disable before stmt triggers
tup = ExecProcNode( (PlanState*)insmtstate);
}
//get the dummy plan node

                        }
			goto mretry;
	                //we have to exhaust the current subplan before moving to the next one.so jump back in to ModifyTable processing
	        }
	        else
	        {
	                MemoryContextSwitchTo(ecxt);
	                PG_RE_THROW();
	        }
	}
	PG_END_TRY();
	return tup;
}


static void
htupdate_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
	ExecEndNode(lsecond(node->custom_ps));
	ExecEndNode(lthird(node->custom_ps));
}

static void
htupdate_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
	ExecReScan(lsecond(node->custom_ps));
	ExecReScan(lthird(node->custom_ps));
}

static CustomExecMethods htupdate_plan_state_methods = {
	.CustomName = "HypertableUpdateState",
	.BeginCustomScan = htupdate_begin,
	.EndCustomScan = htupdate_end,
	.ExecCustomScan = htupdate_exec,
	.ReScanCustomScan = htupdate_rescan,
};

static Node *
htupdate_plan_state_create(CustomScan *cscan)
{
	HypertableUpdateState *state;

	state = (HypertableUpdateState *) newNode(sizeof(HypertableUpdateState), T_CustomScanState);
	state->cscan_state.methods = &htupdate_plan_state_methods;
	state->mt = (ModifyTable *) linitial(cscan->custom_plans);
	state->del_mt = (ModifyTable *) lsecond(cscan->custom_plans);
	state->ins_mt = (ModifyTable *) lthird(cscan->custom_plans);

	return (Node *) state;
}

static CustomScanMethods htupdate_plan_methods = {
	.CustomName = "HyperTableUpdate",
	.CreateCustomScanState = htupdate_plan_state_create,
};

/*
 * HypertableUpdatePlan : this si the plan that is inserted abve ModifyTable when there is a potential partition movement update stmt.
 * This plan internally maintains 2 "fake" internal
 * subplans
 * del_mtplan-handles deletes and ins_mtplan handles
 * inserts.
 * When an update fail due to row movement across 
 * partitions:
 * step 1: del_mtplan is passed the failing
 * tuple for delet. step 2: if the delete succeeds, the 
 * ins_mtplan is called to complete the insert.
 * del_mtplan - is a simple copy of the ModiftTable mt with operation
 *    set as delete on the hypertable and has a subplan that is a HypertableUpdateRedoPlan
 * so we have ModifyTable( DEL on hypertable) -> ChunkDispatch ->
 * HURedoPlan
 * ins_mtplan - modify the operation to a insert for the
 * hypertable and add a subplan with a 
 * ChunkDispatchPlan -> that wraps special HypertableUpdateRedoPlan (has the failing tuple)
 * so we have ModifyTable (INS on hypertable) ->ChunkDispatch->HURedoPlan
 */ 
static Plan *
hypertable_update_plan_create( PlannedStmt *pstmt, ModifyTable *mt, Oid htreloid)
{
	int htridx = -1;
	ModifyTable *del_mtplan , *ins_mtplan;
	ListCell *l;
	Plan *orig_htplan;
	CustomScan *cscan = makeNode(CustomScan);
	Assert(IsA(mt, ModifyTable));

        /*don't think we can rely on nominalRelation being set correctly in mt as we don't have a partitioned table, 
	 * find the correct index for root hypertable into RTE table
	 */
	foreach(l, pstmt->resultRelations)
	{
		int ridx = lfirst_int(l);
	        if( getrelid( ridx, pstmt->rtable) == htreloid )
                {
	           htridx = ridx;
		   break;
                }
        }
	if( htridx == -1 )
        ereport(ERROR,
          (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("hypertable not found in rangetable entries INTERNAL")));
	   
        orig_htplan = list_nth(mt->plans, htridx ); 
	//Lets create 2 dummy ModifyTable plans for INSERT and DELETE
	//ins_mtplan = copyObject(mt);
	//need to fixup the delete plan to look like a delete
	//from a hypertable
	del_mtplan = copyObject(mt);
        del_mtplan->operation  = CMD_DELETE; 	
        del_mtplan->partitioned_rels = NIL; //anyway to free it TODO?
	del_mtplan->resultRelations = list_make1_int(htridx);
	//make subplan for del_mtplan
	{
        	Plan *repl = htupdate_redo_plan_create( orig_htplan);
		del_mtplan->plans = list_make1(repl);
        }	

        //convert modifytable into INSERT ModifyTable for
	//hypertable.	
	ins_mtplan = copyObject(mt);
        ins_mtplan->operation = CMD_INSERT;
        ins_mtplan->partitioned_rels = NIL; //anyway to free it TODO?
	ins_mtplan->resultRelations = list_make1_int(htridx);
        //make the subplan for ins_mtplan
	{
           //use the hypertable root info to create this.
	   Plan *repl = htupdate_redo_plan_create( orig_htplan);
           Plan *cdplan = chunk_dispatch_plan_create1(
			   htreloid, orig_htplan->targetlist, NULL /*clauses */, list_make1(repl) );
	   ins_mtplan->plans = list_make1( cdplan);
	}

	cscan->methods = &htupdate_plan_methods;
	cscan->custom_plans = list_make3(mt, del_mtplan, ins_mtplan);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;
	cscan->scan.plan.targetlist = copyObject(mt->plan.targetlist);

	/* Set the custom scan target list for, e.g., explains */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);

	return &cscan->scan.plan;
}

/* We insert a node between ModifyTableNode and each of its subplans
 * The purpose of this intermediary node is to save the tuple
 * returned by the subplan incase HypertableUpdateState needs to use it
 * So we initially have
 * ModifyTableNode 
 *   |
 *   subplan1 - subplan2 -subplan3
 *
 * The modified plan is
 * HyperTableUpdate
 * |
 * ModifyTable
 * |
 * HyperTableUpdateSubplan1 - HyperTableUpdateSubPlan2 - HTUpdSubPlan3
 * |                                |                      |
 * subplan1                       subplan2                subplan3
 *
 * Note that this gets added only when the ModifyTable
 * involves a UPDATE which can modify partition column
 *
 */

static void
htupdate_subplan_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableUpdateSubplanState *state = (HypertableUpdateSubplanState *) node;
	PlanState  *ps;
	/* now we save subplan state in custom_ps */
	ps = ExecInitNode(state->pl, estate, eflags);

	node->custom_ps = list_make1(ps);
	state->estate = estate;  /* save global estate here for use later */
}

static TupleTableSlot *
htupdate_subplan_exec(CustomScanState *node)
{
	HypertableUpdateSubplanState *state = (HypertableUpdateSubplanState *) node;
	TupleTableSlot *tup = ExecProcNode( linitial(node->custom_ps));
	state->subplan_tup = tup;
	return tup;
}


static void
htupdate_subplan_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
htupdate_subplan_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static CustomExecMethods htupdate_subplan_state_methods = {
	.CustomName = "HypertableUpdateSubplanState",
	.BeginCustomScan = htupdate_subplan_begin,
	.EndCustomScan = htupdate_subplan_end,
	.ExecCustomScan = htupdate_subplan_exec,
	.ReScanCustomScan = htupdate_subplan_rescan,
};

static Node *
htupdate_subplan_state_create(CustomScan *cscan)
{
	HypertableUpdateSubplanState *state;

	state = (HypertableUpdateSubplanState *) newNode(sizeof(HypertableUpdateSubplanState), T_CustomScanState);
	state->cscan_state.methods = &htupdate_subplan_state_methods;
	state->pl = (Plan*)linitial(cscan->custom_plans);

	return (Node *) state;
}

static CustomScanMethods htupdate_subplan_plan_methods = {
	.CustomName = "HypertableUpdateSubplan",
	.CreateCustomScanState = htupdate_subplan_state_create,
};


/*we create a wrapper plan node around the one passed in and return it
 */
static Plan *
hypertable_update_subplan_create( Plan *pl)
{
	CustomScan *cscan = makeNode(CustomScan);
	cscan->methods = &htupdate_subplan_plan_methods;
	cscan->custom_plans = list_make1(pl);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = pl->startup_cost;
	cscan->scan.plan.total_cost = pl->total_cost;
	cscan->scan.plan.plan_rows = pl->plan_rows;
	cscan->scan.plan.plan_width = pl->plan_width;
	cscan->scan.plan.targetlist = copyObject(pl->targetlist);

	/* Set the custom scan target list for, e.g., explains */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);

	return &cscan->scan.plan;
}

/* wrap ModifyTableNode with HyperTableUpdate node
 * also each of ModifyTableNode->plans is wrapped in a 
 * UpdateSubPlanNode which just saves the tuple returned by
 * the underlying plan node
 * Parameters:
 * mt - ModifyTable node that is to be wrapped
 * htreloid - Oid of the parent hypertable
 */
Plan * ts_modify_updpart_plan( PlannedStmt *pstmt, 
		ModifyTable * mt, Oid htreloid)
{
	List *newplanList = NIL;
	ListCell *l;
	Plan *htupd = NULL;
	foreach(l , mt->plans) 
	{
	   Plan *subplan = (Plan*)lfirst(l);
	   Plan *wrapplan = hypertable_update_subplan_create( subplan);
	   /* Note that we should maintain the same order of subplans
	    * as we got from mt->plans
	    * ideally would replace the pointer - cna we do that? TODO
	    */
	   newplanList = lappend( newplanList, wrapplan); // TODO - any better way instead of alloc 1 by 1.

	}
	mt->plans = newplanList;
	htupd 	=  hypertable_update_plan_create( pstmt, mt,  htreloid);
	return htupd;
}

static void
htupdate_redo_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableUpdateRedoState *state = (HypertableUpdateRedoState *) node;
	state->saved_tup = NULL;
}

static TupleTableSlot *
htupdate_redo_exec(CustomScanState *node)
{
	HypertableUpdateRedoState *state = (HypertableUpdateRedoState *) node;
	TupleTableSlot *tup = state->saved_tup;
	state->saved_tup = NULL;
	return tup;
}


static void
htupdate_redo_end(CustomScanState *node)
{
	HypertableUpdateRedoState *state = (HypertableUpdateRedoState *) node;
	state->saved_tup = NULL;
}

static void
htupdate_redo_rescan(CustomScanState *node)
{
	//nothing to do ?
}

static CustomExecMethods htupdate_redo_state_methods = {
	.CustomName = "HypertableUpdateRedoState",
	.BeginCustomScan = htupdate_redo_begin,
	.EndCustomScan = htupdate_redo_end,
	.ExecCustomScan = htupdate_redo_exec,
	.ReScanCustomScan = htupdate_redo_rescan,
};

static Node *
htupdate_redo_plan_state_create(CustomScan *cscan)
{
	HypertableUpdateRedoState *state;

	state = (HypertableUpdateRedoState *) newNode(sizeof(HypertableUpdateRedoState), T_CustomScanState);
	state->cscan_state.methods = &htupdate_redo_state_methods;
	return (Node *) state;
}

static CustomScanMethods htupdate_redo_plan_methods = {
	.CustomName = "HypertableUpdateRedoPlan",
	.CreateCustomScanState = htupdate_redo_plan_state_create,
};


/* HypertableUpdateRedoPlan - 
 * this is a dummy plan that returns the saved tuple
 * (the failed tuple for the update)
 * to ModifyTable node
 * the saved_tuple needs to be saved appropritaley in the corresponding state .
 */
static Plan *
htupdate_redo_plan_create( Plan *pl)
{
	CustomScan *cscan = makeNode(CustomScan);
	cscan->methods = &htupdate_redo_plan_methods;
	cscan->custom_plans = NIL;
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = 0;
	cscan->scan.plan.total_cost = pl->total_cost;
	cscan->scan.plan.plan_rows = 1;
	cscan->scan.plan.plan_width = pl->plan_width;
	cscan->scan.plan.targetlist = copyObject(pl->targetlist);

	/* Set the custom scan target list for, e.g., explains */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);

	return &cscan->scan.plan;
}

