
#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/planner.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "export.h"

#include "plan_partialize.h"

TS_FUNCTION_INFO_V1(ts_partialize);
TS_FUNCTION_INFO_V1(ts_aggregate_transition_type);
TS_FUNCTION_INFO_V1(ts_aggregate_deserialize_fn);
TS_FUNCTION_INFO_V1(ts_aggregate_deserialize);


/*
 * the partialize function mainly serves as a marker that the aggregate called
 * within should return a partial instead of a result. Most of the actual work
 * occurs in the planner, with the actual function just used to ensure the
 * return type is correct.
 */
TSDLLEXPORT Datum
ts_partialize(PG_FUNCTION_ARGS)
{
	Datum		arg;
	Oid			arg_type;
	Oid			send_fn;
	bool		type_is_varlena;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	arg = PG_GETARG_DATUM(0);
	arg_type = get_fn_expr_argtype(fcinfo->flinfo, 0);

	if (arg_type == BYTEAOID)
		PG_RETURN_DATUM(arg);

	getTypeBinaryOutputInfo(arg_type, &send_fn, &type_is_varlena);

	PG_RETURN_BYTEA_P(OidSendFunctionCall(send_fn, arg));

}

/* ///////////////////////////////////// */

typedef struct PartializeWalkerState
{
	bool		found_partialize;
	bool		looking_for_agg;
} PartializeWalkerState;

static bool
partialize_function_call_walker(Node *node, PartializeWalkerState *state)
{
	if (node == NULL)
		return false;

	/*
	 * If the last node we saw was partialize, the next one must be aggregate
	 * we're partializing
	 */
	if (state->looking_for_agg)
	{
		Aggref	   *agg_ref;

		if (!IsA(node, Aggref))
			elog(ERROR, "The input to partialize must be an aggregate");

		agg_ref = castNode(Aggref, node);
		agg_ref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		if (agg_ref->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL))
			agg_ref->aggtype = BYTEAOID;
		else
			agg_ref->aggtype = agg_ref->aggtranstype;

		state->looking_for_agg = false;
	}
	else if (IsA(node, FuncExpr) &&strncmp(get_func_name(castNode(FuncExpr, node)->funcid), "partialize", NAMEDATALEN) == 0)
	{
		state->found_partialize = true;
		state->looking_for_agg = true;
	}

	return expression_tree_walker((Node *) node, partialize_function_call_walker, state);
}

/* We currently cannot handle cases like
 *     SELECT sum(i), partialize(sum(i)) ...
 * instead we use this function to ensure that if any of the aggregates in a staetment are partialized, all of them are
 */
static bool
ensure_only_partials(Node *node, void *state)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref) &&castNode(Aggref, node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		elog(ERROR, "Cannot mix partialized and non-partialized aggregates in the same statement");

	return expression_tree_walker((Node *) node, ensure_only_partials, state);
}

void
plan_add_partialize(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
	PartializeWalkerState state = {
		.found_partialize = false,
		.looking_for_agg = false,
	};
	ListCell   *lc;

	if (CMD_SELECT != parse->commandType)
		return;

	expression_tree_walker((Node *) parse->targetList, partialize_function_call_walker, &state);

	if (state.found_partialize)
	{
		expression_tree_walker((Node *) parse->targetList, ensure_only_partials, NULL);

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = lfirst(lc);

			if (IsA(path, AggPath))
				((AggPath *) path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}

		foreach(lc, output_rel->pathlist)
		{
			Path	   *path = lfirst(lc);

			if (IsA(path, AggPath))
				((AggPath *) path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}
	}
}

/* ///////////////////////////////////// */

Datum
ts_aggregate_transition_type(PG_FUNCTION_ARGS)
{
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	Oid			trans_type;
	Oid			agg_fn_oid = PG_GETARG_OID(0);

	/* fetch aggregate entry from pg_aggregate */
	aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg_fn_oid));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "invalid aggregate function '%s'", get_func_name(agg_fn_oid));

	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	trans_type = aggform->aggtranstype;
	ReleaseSysCache(aggTuple);

	PG_RETURN_OID(trans_type);
}


Datum
ts_aggregate_deserialize_fn(PG_FUNCTION_ARGS)
{
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	Oid			deserialize_fn = InvalidOid;
	Oid			agg_fn_oid = PG_GETARG_OID(0);

	/* fetch aggregate entry from pg_aggregate */
	aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg_fn_oid));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "invalid aggregate function '%s'", get_func_name(agg_fn_oid));

	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	if (OidIsValid(aggform->aggdeserialfn))
		deserialize_fn = aggform->aggdeserialfn;
	ReleaseSysCache(aggTuple);

	PG_RETURN_OID(deserialize_fn);
}

Datum
ts_aggregate_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *serialized = PG_GETARG_BYTEA_P(0);
	RegProcedure agg = PG_GETARG_OID(1);
	Oid			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	Datum		deserialized;

	aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "invalid aggregate function '%s'", get_func_name(agg));

	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	if (arg_type != aggform->aggtranstype)
		/* elog(ERROR, "invalid return type, got %s expected %s", ); */
		elog(ERROR, "invalid return type, got %d expected %d", arg_type, aggform->aggtranstype);

	if (OidIsValid(aggform->aggdeserialfn))
	{
		FmgrInfo	deserialize_finfo;

		fmgr_info(aggform->aggdeserialfn, &deserialize_finfo);

		deserialized = FunctionCall1Coll(&deserialize_finfo, InvalidOid, PointerGetDatum(serialized));
	}
	else
	{
		StringInfo string = makeStringInfo();
		Oid			recv_fn,
					typIOParam;

		getTypeBinaryInputInfo(arg_type, &recv_fn, &typIOParam);

		appendBinaryStringInfo(
							   string, VARDATA_ANY(serialized), VARSIZE_ANY_EXHDR(serialized));

		deserialized = OidReceiveFunctionCall(recv_fn, string, typIOParam, 0);
	}
	PG_RETURN_DATUM(deserialized);
}
