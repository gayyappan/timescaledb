/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_aggregate.h>
//#include <nodes/value.h>
#include <utils/syscache.h>
#include <utils/datum.h>
#include <utils/builtins.h>
#include <access/htup_details.h> //GETSTRUCT
//#include <lib/stringinfo.h>
//#include <libpq/pqformat.h>

#include "compat.h"

TS_FUNCTION_INFO_V1(ts_caggfinal_sfunc);
// TS_FUNCTION_INFO_V1(ts_caggfinal_combinefunc);
// TS_FUNCTION_INFO_V1(ts_caggfinal_combinefunc);
TS_FUNCTION_INFO_V1(ts_caggfinal_finalfunc);

typedef struct CAggInternalAggState
{
	Oid aggtransfn;
	Oid finalfnoid;
	Oid combinefnoid;
	Oid serialfnoid;
	Oid deserialfnoid;
	Oid transtype;
	Datum agg_state;
	bool agg_state_isnull;
	bool agg_state_comb_init;
	/*when we have a strict combine function, both arg values have
	 * to eb non-null (like min/max). When we see first non-null
	 * value, initilaize it and set this to be true.
	 */
	FmgrInfo deserialfn;
	FmgrInfo combinefn;
	FmgrInfo finalfn;
	FunctionCallInfoData deserfn_fcinfo;
	FunctionCallInfoData combfn_fcinfo;
	FunctionCallInfoData finalfn_fcinfo;
} CAggInternalAggState;

static TupleTableSlot *
dummy_execagg(PlanState *node)
{
	elog(ERROR, "dummy_execagg invoked");
}

static AggState *
createDummyAggState()
{
	AggState *aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = NULL;
	aggstate->ss.ps.state = NULL;
	aggstate->ss.ps.ExecProcNode = dummy_execagg;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->numtrans = 0;
	aggstate->maxsets = 0;
	aggstate->projected_set = -1;
	aggstate->current_set = 0;
	aggstate->peragg = NULL;
	aggstate->pertrans = NULL;
	aggstate->curperagg = NULL;
	aggstate->curpertrans = NULL;
	aggstate->input_done = false;
	aggstate->agg_done = false;
	aggstate->pergroup = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->sort_in = NULL;
	aggstate->sort_out = NULL;

	return aggstate;
}

// TODO -deal with NULLS
static Datum
deserialize_aggstate(CAggInternalAggState *cstate, bytea *serialized, bool serialized_isnull,
					 bool *deserisnull)
{
	Datum deserialized;
	FunctionCallInfoData *agg_fcinfo = &cstate->deserfn_fcinfo;
	*deserisnull = true;
	if (OidIsValid(cstate->deserialfnoid))
	{
		if (serialized_isnull && cstate->deserialfn.fn_strict)
		{
			PG_RETURN_DATUM(deserialized);
			/*don't call the deser function */
		}
		agg_fcinfo->arg[0] = PointerGetDatum(serialized);
		agg_fcinfo->argnull[0] = serialized_isnull;
		deserialized = FunctionCallInvoke(agg_fcinfo);
		*deserisnull = agg_fcinfo->isnull;
		if (agg_fcinfo->isnull) // TODO remove
			elog(NOTICE, "got NULL in deserialize_aggstate");
	}
	else if (!serialized_isnull)
	{
		StringInfo string = makeStringInfo();
		Oid recv_fn, typIOParam;

		getTypeBinaryInputInfo(cstate->transtype, &recv_fn, &typIOParam);

		appendBinaryStringInfo(string, VARDATA_ANY(serialized), VARSIZE_ANY_EXHDR(serialized));

		deserialized = OidReceiveFunctionCall(recv_fn, string, typIOParam, 0);
		*deserisnull = false;
	}
	PG_RETURN_DATUM(deserialized);
}

static CAggInternalAggState *
caggfinal_initstate(MemoryContext *aggcontext, Oid aggfnoid, Oid collation, AggState *orig_aggstate)
{
	CAggInternalAggState *cstate = NULL;
	HeapTuple aggTuple;
	Form_pg_aggregate aggform;
	cstate = (CAggInternalAggState *) MemoryContextAlloc(*aggcontext, sizeof(CAggInternalAggState));
	/* Fetch the pg_aggregate row */
	aggTuple = SearchSysCache1(AGGFNOID, aggfnoid);
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "cache lookup failed for aggregate %u", aggfnoid);
	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	cstate->aggtransfn = aggform->aggtransfn;
	cstate->finalfnoid = aggform->aggfinalfn;
	cstate->combinefnoid = aggform->aggcombinefn;
	cstate->serialfnoid = aggform->aggserialfn;
	cstate->deserialfnoid = aggform->aggdeserialfn;
	cstate->transtype = aggform->aggtranstype;
	// Need to init cstate->agg_state
	cstate->agg_state_isnull = true;
	cstate->agg_state_comb_init = false;
	ReleaseSysCache(aggTuple);
	{
		void *aggstate_cxt;
		if (OidIsValid(cstate->deserialfnoid))
		{
			fmgr_info(cstate->deserialfnoid, &cstate->deserialfn);
			/* pass the aggstate information from our current call context */
			//		aggstate_cxt = aggcontext;
			aggstate_cxt = createDummyAggState();
			InitFunctionCallInfoData(cstate->deserfn_fcinfo,
									 &cstate->deserialfn,
									 1,
									 collation,
									 (void *) aggstate_cxt,
									 NULL);
		}
		if (OidIsValid(cstate->combinefnoid))
		{
			fmgr_info(cstate->combinefnoid, &cstate->combinefn);
			/* pass the aggstate information from our current call context */
			aggstate_cxt = orig_aggstate;
			InitFunctionCallInfoData(cstate->combfn_fcinfo,
									 &cstate->combinefn,
									 2,
									 collation,
									 (void *) aggstate_cxt,
									 NULL);
		}
		if (OidIsValid(cstate->finalfnoid))
		{
			fmgr_info(cstate->finalfnoid, &cstate->finalfn);
			/* pass the aggstate information from our current call context */
			aggstate_cxt = orig_aggstate;
			InitFunctionCallInfoData(cstate->finalfn_fcinfo,
									 &cstate->finalfn,
									 1,
									 collation,
									 (void *) aggstate_cxt,
									 NULL);
		}
	}
	return cstate;
}

/* cagg_final(internal internal_state, Oid aggregatefun oid,
 *            Oid aggref_inputcollid, bytea aggstate,
 * 		ANYELEMENT null --for type inference )
 */
Datum
ts_caggfinal_sfunc(PG_FUNCTION_ARGS)
{
	CAggInternalAggState *cstate =
		PG_ARGISNULL(0) ? NULL : (CAggInternalAggState *) PG_GETARG_POINTER(0);
	Oid aggfnoid = PG_GETARG_OID(1);
	Oid inpcollid = PG_GETARG_OID(2);
	/* the arg is a internal state representation for the
	 * agg we are computing
	 */
	// Datum aggstate_arg = PG_GETARG_DATUM(3); //how do we deal with NULLs?
	bytea *aggstate_arg_serial = PG_ARGISNULL(3) ? NULL : PG_GETARG_BYTEA_P(3);
	bool aggstate_arg_isnull = PG_ARGISNULL(3) ? true : false;
	MemoryContext aggcontext, old_context;
	Datum aggstate_arg_deser;
	AggState *orig_aggstate;
	Assert(IsA(fcinfo->context, AggState));
	orig_aggstate = (AggState *) fcinfo->context;
	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "cagg_final_sfunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(aggcontext);

	if (cstate == NULL)
	{
		cstate = caggfinal_initstate(&aggcontext, aggfnoid, inpcollid, orig_aggstate);
		/* intial state = aggstate from first invocation */
		cstate->agg_state = deserialize_aggstate(cstate,
												 aggstate_arg_serial,
												 aggstate_arg_isnull,
												 &cstate->agg_state_isnull);
		cstate->agg_state_comb_init = !(cstate->agg_state_isnull);
	}
	else
	{
		bool deser_isnull;
		bool callcomb;
		aggstate_arg_deser =
			deserialize_aggstate(cstate, aggstate_arg_serial, aggstate_arg_isnull, &deser_isnull);
		/* don't combine state with NULL arg when we have a strict
		 * function
		 */
		callcomb = true;
		if (cstate->combinefn.fn_strict)
		{
			if (cstate->agg_state_comb_init == false && deser_isnull == false)
			{
				cstate->agg_state = aggstate_arg_deser;
				cstate->agg_state_isnull = false;
				cstate->agg_state_comb_init = true;
				callcomb = false;
			}
			else if (deser_isnull)
				callcomb = false;
		}
		// if( !( cstate->combinefn.fn_strict && deser_isnull ))
		if (callcomb)
		{
			cstate->combfn_fcinfo.arg[0] = cstate->agg_state;
			cstate->combfn_fcinfo.argnull[0] = cstate->agg_state_isnull;
			cstate->combfn_fcinfo.arg[1] = aggstate_arg_deser;
			cstate->combfn_fcinfo.argnull[1] = deser_isnull;
			cstate->agg_state = FunctionCallInvoke(&cstate->combfn_fcinfo);
			cstate->agg_state_isnull = cstate->combfn_fcinfo.isnull;
		}
	}
	MemoryContextSwitchTo(old_context);

	PG_RETURN_POINTER(cstate);
}

/* do we need this , for debugging for now
 * first_combinerfunc(internal, internal) => internal */
/*
static Datum
ts_caggfinal_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	FmgrInfo	aggcomb_finfo;

	CAggInternalAggState *cstate1 = PG_ARGISNULL(0) ? NULL :
		(CAggInternalAggState *) PG_GETARG_POINTER(0);
	CAggInternalAggState *cstate2 = PG_ARGISNULL(1) ? NULL :
		(CAggInternalAggState *) PG_GETARG_POINTER(0);
	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		elog(ERROR, "ts_caggfinal_combinefunc called in non-aggregate context");
	}
	fmgr_info( cstate->combinefnoid, &aggcomb_finfo);
	cstate1->agg_state = FunctionCall2Coll(&aggcomb_finfo, InvalidOid,
			cstate1 ? cstate1->agg_state : NULL,
			cstate2 ? cstate2->agg_state : NULL);
	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(cstate1);
}
*/

/* ts_bookend_finalfunc(internal, Oid, "any", anyelement) => anyelement */
Datum
ts_caggfinal_finalfunc(PG_FUNCTION_ARGS)
{
	CAggInternalAggState *cstate =
		PG_ARGISNULL(0) ? NULL : (CAggInternalAggState *) PG_GETARG_POINTER(0);
	MemoryContext aggcontext, old_context;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "cagg_final_finalfunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(aggcontext);
	if (OidIsValid(cstate->finalfnoid))
	{
		if (!(cstate->finalfn.fn_strict && cstate->agg_state_isnull))
		{
			cstate->finalfn_fcinfo.arg[0] = cstate->agg_state;
			cstate->finalfn_fcinfo.argnull[0] = cstate->agg_state_isnull;
			cstate->agg_state = FunctionCallInvoke(&cstate->finalfn_fcinfo);
			cstate->agg_state_isnull = cstate->finalfn_fcinfo.isnull;
		}
	}
	MemoryContextSwitchTo(old_context);
	if (cstate->agg_state_isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(cstate->agg_state);
}
