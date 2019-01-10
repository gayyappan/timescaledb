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

#include <hypertable_update.h>

static Node * htupdate_state_create(CustomScan *cscan);

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
}

static TupleTableSlot *
htupdate_exec(CustomScanState *node)
{
	TupleTableSlot * tup = NULL;
        MemoryContext ccxt = CurrentMemoryContext;
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
                        FlushErrorState();
			/* can we recover from here and start a new exec path */
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
}

static void
htupdate_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static CustomExecMethods htupdate_state_methods = {
	.CustomName = "HypertableUpdateState",
	.BeginCustomScan = htupdate_begin,
	.EndCustomScan = htupdate_end,
	.ExecCustomScan = htupdate_exec,
	.ReScanCustomScan = htupdate_rescan,
};

static Node *
htupdate_state_create(CustomScan *cscan)
{
	HypertableUpdateState *state;

	state = (HypertableUpdateState *) newNode(sizeof(HypertableUpdateState), T_CustomScanState);
	state->cscan_state.methods = &htupdate_state_methods;
	state->mt = (ModifyTable *) linitial(cscan->custom_plans);

	return (Node *) state;
}

static CustomScanMethods htupdate_plan_methods = {
        .CustomName = "HyperTableUpdate",
        .CreateCustomScanState = htupdate_state_create,
};



Plan *
ts_hypertable_update_plan_create( ModifyTable *mt)
{
	CustomScan *cscan = makeNode(CustomScan);
	Assert(IsA(mt, ModifyTable));

	cscan->methods = &htupdate_plan_methods;
	cscan->custom_plans = list_make1(mt);
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
Plan *
ts_hypertable_update_subplan_create( Plan *pl)
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

