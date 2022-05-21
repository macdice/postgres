\echo Use "CREATE EXTENSION slrubench" to load this file. \quit

CREATE FUNCTION test_clog_fetch(x1 pg_catalog.xid,
								x2 pg_catalog.xid,
								loops pg_catalog.int4)
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;
