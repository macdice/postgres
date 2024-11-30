-- Exercise the ValidateClusterCatalogString() calls that should cover all
-- entry points (a few cases have ASCII-only validation of their own and give
-- slightly different error messages).
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
SHOW CLUSTER CATALOG ENCODING;

-- pg_authid
CREATE USER regress_astérix;
CREATE USER regress_fred;
ALTER USER regress_fred RENAME TO regress_astérix;
DROP USER regress_fred;

-- pg_database
CREATE DATABASE regression_café;
ALTER DATABASE template1 RENAME TO regression_café;
CREATE DATABASE regression_ok TEMPLATE template0 LOCALE 'français';

-- pg_db_role_setting
CREATE USER regress_fred;
ALTER ROLE regress_fred SET application_name TO 'café';
DROP USER regress_fred;

-- pg_parameter_acl
-- XXX

-- pg_replication_origin
SELECT pg_replication_origin_create('regress_café');

-- pg_shdescription
COMMENT ON DATABASE template0 IS 'café';
-- non-shared objects are OK, because non-shared catalog
COMMENT ON TABLE pg_catalog.pg_class IS 'café';
COMMENT ON TABLE pg_catalog.pg_class IS NULL;

-- pg_shseclabel
-- XXX

-- pg_subscription
CREATE SUBSCRIPTION regress_café CONNECTION 'dbname=crême' PUBLICATION brûlée;
CREATE SUBSCRIPTION regress_ok   CONNECTION 'dbname=crême' PUBLICATION brûlée;
CREATE SUBSCRIPTION regress_ok   CONNECTION 'dbname=ok'    PUBLICATION brûlée;
CREATE SUBSCRIPTION regress_ok   CONNECTION 'dbname=ok'    PUBLICATION ok      WITH (slot_name = 'café');
CREATE SUBSCRIPTION regress_ok   CONNECTION 'dbname=ok'    PUBLICATION ok      WITH (synchronous_commit = 'café');
CREATE SUBSCRIPTION regress_ok   CONNECTION 'dbname=ok'    PUBLICATION ok      WITH (origin = 'café');

-- pg_tablespace
SET allow_in_place_tablespaces = 'on';
CREATE TABLESPACE regress_café LOCATION '';
CREATE TABLESPACE regress_ok LOCATION '';
ALTER TABLESPACE regress_ok RENAME TO regress_café;
DROP TABLESPACE regress_ok;

-- Check that we can create a new database with a different encoding,
-- while the shared catalog encoding is ASCII
CREATE DATABASE regression_latin1 TEMPLATE template0 LOCALE 'C' ENCODING LATIN1;

-- Check that we can't change the shared catalog encoding to UTF8, because that
-- LATIN1 database is in the way, then drop it so we can.
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO DATABASE;
DROP DATABASE regression_latin1;
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO DATABASE;

-- Test that we can now do each of those things that failed before, and that
-- those things block us from going back to ASCII.

-- pg_authid
CREATE USER regress_astérix;
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
DROP USER regress_astérix;

-- pg_database
CREATE DATABASE regression_café;
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
DROP DATABASE regression_café;
-- but we can't make a LATIN1 database while we have UTF8 catalogs
CREATE DATABASE regression_latin1 TEMPLATE template0 LOCALE 'C' ENCODING LATIN1;

-- pg_db_role_setting
CREATE USER regress_fred;
ALTER ROLE regress_fred SET application_name TO 'café';
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
DROP USER regress_fred;

-- pg_parameter_acl
-- XXX

-- pg_replication_origin
SELECT pg_replication_origin_create('regress_café');
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
SELECT pg_replication_origin_drop('regress_café');

-- pg_shdescription
COMMENT ON DATABASE template0 IS 'café';
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
COMMENT ON DATABASE template0 IS 'unmodifiable empty database';

-- pg_shseclabel
-- XXX

-- pg_subscription
-- XXX

-- pg_tablespace
CREATE TABLESPACE regress_café LOCATION '';
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
DROP TABLESPACE regress_café;

-- We dropped everything that was in the way, so we should be able to go back
-- to ASCII now.
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;

-- Try out UNDEFINED mode, which is the only way to have a non-ASCII database
-- name and mutiple encodings at the same time
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO UNDEFINED;
SHOW CLUSTER CATALOG ENCODING;
CREATE DATABASE regression_café ENCODING UTF8;
CREATE DATABASE regression_latin1 TEMPLATE template0 LOCALE 'C' ENCODING LATIN1;

-- We can't switch to ASCII from this state
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
SHOW CLUSTER CATALOG ENCODING;

-- We also can't switch to UTF8 from this state
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO DATABASE;
SHOW CLUSTER CATALOG ENCODING;

-- If we get rid of the LATIN1 database, we can go to UTF8
DROP DATABASE regression_latin1;
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO DATABASE;
SHOW CLUSTER CATALOG ENCODING;

-- We still can't go back to ASCII unless we also get rid of the non-ASCII
-- database name.
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;
DROP DATABASE regression_café;
ALTER SYSTEM SET CLUSTER CATALOG ENCODING TO ASCII;

