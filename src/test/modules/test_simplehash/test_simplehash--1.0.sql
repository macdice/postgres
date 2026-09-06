/* src/test/modules/test_simlehash/test_simplehash--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_simplehash" to load this file. \quit

CREATE FUNCTION test_simplehash()
RETURNS void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
