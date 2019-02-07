-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- chunk - the OID of the chunk to be CLUSTERed
-- index - the OID of the index to be CLUSTERed on, or NULL to use the index
--         last used
CREATE OR REPLACE FUNCTION reorder_chunk(
    chunk REGCLASS,
    index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_reorder_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION partialize(arg ANYELEMENT)
RETURNS BYTEA AS '@MODULE_PATHNAME@', 'ts_partialize' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION aggregate_transition_type(arg REGPROCEDURE)
RETURNS REGTYPE AS '@MODULE_PATHNAME@', 'ts_aggregate_transition_type' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE FUNCTION aggregate_deserialize_fn(arg REGPROCEDURE)
RETURNS REGPROC AS '@MODULE_PATHNAME@', 'ts_aggregate_deserialize_fn' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION aggregate_deserialize(val BYTEA, agg REGPROCEDURE, type ANYELEMENT)
RETURNS ANYELEMENT AS '@MODULE_PATHNAME@', 'ts_aggregate_deserialize' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.ts_caggfinal_sfunc(
internal, Oid, Oid, BYTEA , ANYELEMENT)
RETURNS internal 
AS '@MODULE_PATHNAME@', 'ts_caggfinal_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.ts_caggfinal_finalfunc(
internal, Oid, Oid, BYTEA  , ANYELEMENT)
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'ts_caggfinal_finalfunc'
LANGUAGE C IMMUTABLE ;

