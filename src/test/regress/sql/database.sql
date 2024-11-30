CREATE DATABASE regression_tbd
	LC_COLLATE "C" LC_CTYPE "C" TEMPLATE template0;
ALTER DATABASE regression_tbd RENAME TO regression_xyz;
ALTER DATABASE regression_xyz SET TABLESPACE regress_tblspace;
ALTER DATABASE regression_xyz RESET TABLESPACE;
ALTER DATABASE regression_xyz CONNECTION_LIMIT 123;

-- Test PgDatabaseToastTable.  Doing this with GRANT would be slow.
BEGIN;
UPDATE pg_database
SET datacl = array_fill(makeaclitem(10, 10, 'USAGE', false), ARRAY[5e5::int])
WHERE datname = 'regression_xyz';
-- load catcache entry, if nothing else does
ALTER DATABASE regression_xyz RESET TABLESPACE;
ROLLBACK;

DROP DATABASE regression_xyz;
