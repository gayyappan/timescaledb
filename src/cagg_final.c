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
#include <access/htup_details.h>        //GETSTRUCT
//#include <lib/stringinfo.h>
//#include <libpq/pqformat.h>

#include "compat.h"

TS_FUNCTION_INFO_V1(ts_caggfinal_sfunc);
//TS_FUNCTION_INFO_V1(ts_caggfinal_combinefunc);
//TS_FUNCTION_INFO_V1(ts_caggfinal_combinefunc);
TS_FUNCTION_INFO_V1(ts_caggfinal_finalfunc);

typedef struct CAggInternalAggState
{
                Oid                     aggtransfn;
                Oid                     aggfinalfn;
                Oid                     aggcombinefn;
                Oid                     aggserialfn;
                Oid                     aggdeserialfn;
		Oid			transtype;
		Datum			agg_state;
} CAggInternalAggState;

static Datum deserialize_aggstate( Oid aggdeserialfn,  Oid transtype,
		bytea *serialized)
{
	Datum deserialized;
        if (OidIsValid(aggdeserialfn))
        {
                FmgrInfo        deserialize_finfo;

                fmgr_info(aggdeserialfn, &deserialize_finfo);

                deserialized = FunctionCall1Coll(&deserialize_finfo, InvalidOid, PointerGetDatum(serialized));
        }
        else
        {
                StringInfo string = makeStringInfo();
                Oid                     recv_fn,
                                        typIOParam;

                getTypeBinaryInputInfo(transtype, &recv_fn, &typIOParam);

                appendBinaryStringInfo(
                                                           string, VARDATA_ANY(serialized), VARSIZE_ANY_EXHDR(serialized));

  deserialized = OidReceiveFunctionCall(recv_fn, string, typIOParam, 0);
	}
	PG_RETURN_DATUM(deserialized);	

}

/* cagg_final(internal internal_state, Oid aggregatefun oid,  bytea aggstate,
 * ANYELEMENT null ) */
Datum
ts_caggfinal_sfunc(PG_FUNCTION_ARGS)
{
	CAggInternalAggState *cstate = PG_ARGISNULL(0) ? NULL : 
		(CAggInternalAggState *) PG_GETARG_POINTER(0);
	Oid  aggfnoid = PG_GETARG_OID(1);
	/* the arg is a internal state representation for the
	 * agg we are computing
	 */
	//Datum aggstate_arg = PG_GETARG_DATUM(3); //how do we deal with NULLs?
	bytea *aggstate_arg_serial = PG_GETARG_BYTEA_P(2);
	MemoryContext aggcontext, old_context;
	FmgrInfo	aggcomb_finfo;
	Datum aggstate_arg_deser;
	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "cagg_final_sfunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(aggcontext);

	if (cstate == NULL)
	{
                HeapTuple       aggTuple;
                Form_pg_aggregate aggform;
		Datum initVal;
		Datum textInitVal;
		bool initValIsNull;
		cstate = (CAggInternalAggState *) MemoryContextAlloc(aggcontext, sizeof(CAggInternalAggState));
               /* Fetch the pg_aggregate row */
                aggTuple = SearchSysCache1(AGGFNOID, aggfnoid);
                if (!HeapTupleIsValid(aggTuple))
                        elog(ERROR, "cache lookup failed for aggregate %u", aggfnoid);
                aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
                cstate->aggtransfn = aggform->aggtransfn;
                cstate->aggfinalfn = aggform->aggfinalfn;
                cstate->aggcombinefn = aggform-> aggcombinefn;
                cstate->aggserialfn = aggform->aggserialfn;
                cstate->aggdeserialfn = aggform->aggdeserialfn;
		cstate->transtype = aggform->aggmtranstype;
                 /*
                  * initval is potentially null, so don't access it as a struct
		  * use SysCacheGetAttr
		  */
                 textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
                                 Anum_pg_aggregate_agginitval, &initValIsNull);
                 if (initValIsNull)
                         initVal = (Datum) 0;
                 else
		 {
		        Oid                     typinput, typioparam;
		        char       *strInitVal;
    
   		        getTypeInputInfo(cstate->transtype, &typinput, &typioparam);
		        strInitVal = TextDatumGetCString(textInitVal);
		        initVal = OidInputFunctionCall(typinput, strInitVal,
                                                              typioparam, -1);
        		pfree(strInitVal);
		 }

		cstate->agg_state = initVal;
		ReleaseSysCache(aggTuple);
	}

        aggstate_arg_deser = deserialize_aggstate( 
		cstate->aggdeserialfn,  cstate->transtype, aggstate_arg_serial);
	//now we cna combine state information
	fmgr_info( cstate->aggcombinefn, &aggcomb_finfo);
	cstate->agg_state = FunctionCall2Coll(&aggcomb_finfo, InvalidOid, 
			cstate->agg_state ,
			aggstate_arg_deser);
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
	fmgr_info( cstate->aggcombinefn, &aggcomb_finfo);
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
	CAggInternalAggState *cstate = PG_ARGISNULL(0) ? NULL : 
		(CAggInternalAggState *) PG_GETARG_POINTER(0);
	MemoryContext aggcontext, old_context;
	FmgrInfo	aggfinal_finfo;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "cagg_final_finalfunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(aggcontext);
	if( OidIsValid( cstate->aggfinalfn ) )
	{
		fmgr_info(cstate->aggfinalfn, &aggfinal_finfo);
		cstate->agg_state = FunctionCall1Coll(&aggfinal_finfo, InvalidOid, 
			cstate->agg_state );
	}
	MemoryContextSwitchTo(old_context);

	PG_RETURN_DATUM(cstate->agg_state);
}
