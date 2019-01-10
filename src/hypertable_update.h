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

typedef struct HypertableUpdateState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableUpdateState;

typedef struct HypertableUpdateSubplanState
{
	CustomScanState cscan_state;
	Plan *pl;
	TupleTableSlot *subplan_tup;  /* a place to save subplan tuple 
					 for later use by
					 HypertableUpdateState
                                       */
} HypertableUpdateSubplanState;

extern Plan *ts_hypertable_update_plan_create( ModifyTable *mt);
extern Plan *ts_hypertable_update_subplan_create( Plan *pl);

#endif							/* TIMESCALEDB_HYPERTABLE_UPDATE_H */
