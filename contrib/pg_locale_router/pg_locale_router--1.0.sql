/* contrib/pg_locale_router/pg_locale_router--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_locale_router" to load this file. \quit

CREATE FUNCTION pg_locale_router_icu_libraries(
       OUT major_version int,
       OUT icu_version text,
       OUT unicode_version text,
       OUT ref_count int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_locale_router_icu_libraries'
LANGUAGE C STRICT;

CREATE FUNCTION pg_locale_router_icu_locales(
       major_version int,
       OUT locale text,
       OUT collate_version text,
       OUT uca_version text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_locale_router_icu_locales'
LANGUAGE C STRICT;
