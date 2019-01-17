/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_UPDATE_H
#define TIMESCALEDB_HYPERTABLE_UPDATE_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"
#include <chunk_dispatch_state.h>

typedef struct HypertableUpdateState
{
	CustomScanState cscan_state;
	Oid htoid;
	ModifyTable *mt;
	ModifyTable *del_mt;
	ModifyTable *ins_mt;
	EState *estate ; //do we need this
	ChunkDispatch *dispatch;
} HypertableUpdateState;

typedef struct HypertableUpdateSubplanState
{
	CustomScanState cscan_state;
	Plan *pl;
	TupleTableSlot *subplan_tup;  /* a place to save subplan tuple 
					 for later use by
					 HypertableUpdateState
                                       */
	EState *estate;
} HypertableUpdateSubplanState;

typedef struct HypertableUpdateRedoState
{
	CustomScanState cscan_state;
	TupleTableSlot *saved_tup;
} HypertableUpdateRedoState;
	
//extern Plan *ts_hypertable_update_plan_create( PlannedStmt *pstmt, ModifyTable *mt);
//extern Plan *ts_hypertable_update_subplan_create( Plan *pl);
extern Plan* ts_modify_updpart_plan( PlannedStmt *pstmt,
                ModifyTable * mt, Oid htreloid);
#endif							/* TIMESCALEDB_HYPERTABLE_UPDATE_H */
