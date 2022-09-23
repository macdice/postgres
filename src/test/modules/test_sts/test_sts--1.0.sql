/* src/test/modules/test_sts/test_sts--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_sts" to load this file. \quit

CREATE FUNCTION test_sts(n int, size int)
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
