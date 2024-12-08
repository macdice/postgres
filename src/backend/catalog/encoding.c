/*-------------------------------------------------------------------------
 *
 * encoding.c
 *		Shared catalog encoding management.
 *
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/encoding.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_parameter_acl.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "common/string.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"

bool
StringIsValidInClusterEncoding(const char *s, int cluster_encoding)
{
	/* In UNDEFINED mode, it is valid by definition. */
	if (cluster_encoding == CLUSTER_ENCODING_UNDEFINED)
		return true;

	/*
	 * Despite using the value PG_SQL_ASCII (which in other contexts means
	 * anything-goes), here we accept only 7-bit ASCII because only 7-bit
	 * ASCII is a subset of all server encodings.
	 */
	if (cluster_encoding == CLUSTER_ENCODING_ASCII)
		return pg_is_ascii(s);

	/*
	 * Otherwise it has to match the database encoding.  Since this function
	 * handles externally sourced strings, we validate even database encoding
	 * instead of assuming it is valid.
	 */
	return pg_encoding_verifymbstr(cluster_encoding, s, strlen(s));
}

bool
StringIsValidInCurrentClusterEncoding(const char *s)
{
	/* XXX think about locking */
	return StringIsValidInClusterEncoding(s, GetClusterEncoding());
}

/*
 * Check if a NULL-terminated string can be inserted into a shared catalog,
 * reporting an error it not.  The caller must hold a lock on the shared
 * catalog table, to block AlterSystemSetClusterEncoding().
 */
void
ValidateSharedCatalogString(Relation rel, const char *s)
{
	int cluster_encoding;

	/*
	 * The rel argument helps to make sure that caller remembered to lock the
	 * catalog before validating strings to be inserted.  We can use it to
	 * create a better error message below.
	 */
	Assert(rel->rd_rel->relisshared);
	cluster_encoding = GetClusterEncoding();

	/* UNDEFINED is valid by definition. */
	if (cluster_encoding == CLUSTER_ENCODING_UNDEFINED)
		return;

	/* ASCII requires explicit validation. */
	if (cluster_encoding == CLUSTER_ENCODING_ASCII)
	{		
		if (!pg_is_ascii(s))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("the value \"%s\" contains invalid characters for shared catalog \"%s\"",
							s,
							RelationGetRelationName(rel)),
					 errdetail("CLUSTER ENCODING is set to ASCII."),
					 errhint("Consider ALTER SYSTEM SET CLUSTER ENCODING TO DATABASE.")));
		return;
	}

	/*
	 * Otherwise the string must be valid in the database encoding, because it
	 * came from a session connected to the database.
	 */
	Assert(cluster_encoding == GetClusterEncoding());
}

/*
 * Try to change the cluster encoding, if all the conditions are met.
 */
void
AlterSystemSetClusterEncoding(const char *encoding_name)
{
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tup;
	int			encoding;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied")));

	/* Decode the name. */
	if (pg_strcasecmp(encoding_name, "DATABASE") == 0)
		encoding = GetDatabaseEncoding();
	else if (pg_strcasecmp(encoding_name, "ASCII") == 0)
		encoding = CLUSTER_ENCODING_ASCII;
	else if (pg_strcasecmp(encoding_name, "UNDEFINED") == 0)
		encoding = CLUSTER_ENCODING_UNDEFINED;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid cluster encoding: %s",
						encoding_name)));

	/*
	 * Lock all of the shared catalog tables containing name or text values,
	 * to prevent concurrent updates.  This is the set of shared catalogs that
	 * contain text.  If new shared catalogs are invented that hold text they
	 * will need to be handled here too.  For every validation that we perform
	 * below, there must also be corresponding calls to
	 * ValidateSharedCatalogString() in the commands that CREATE or ALTER
	 * these database objects.
	 */
	LockRelationOid(AuthIdRelationId, AccessExclusiveLock);
	LockRelationOid(DatabaseRelationId, AccessExclusiveLock);
	LockRelationOid(DbRoleSettingRelationId, AccessExclusiveLock);
	LockRelationOid(ParameterAclRelationId, AccessExclusiveLock);
	LockRelationOid(ReplicationOriginRelationId, AccessExclusiveLock);
	LockRelationOid(SharedDescriptionRelationId, AccessExclusiveLock);
	LockRelationOid(SharedSecLabelRelationId, AccessExclusiveLock);
	LockRelationOid(SubscriptionRelationId, AccessExclusiveLock);
	LockRelationOid(TableSpaceRelationId, AccessExclusiveLock);

	/* No change? */
	if (GetClusterEncoding() == encoding)
		return;

	if (encoding == CLUSTER_ENCODING_UNDEFINED)
	{
		/* There are no encoding restrictions for UNDEFINED.  Good luck. */
	}
	else if (encoding == CLUSTER_ENCODING_ASCII)
	{
		/* Make sure all the shared GUCs contain only pure 7-bit ASCII. */
		ValidateSharedGucEncoding(ERROR, encoding);
		
		/* Make sure all shared catalogs contain only pure 7-bit ASCII. */

		/* pg_authid */
		rel = table_open(AuthIdRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			Form_pg_authid authid = (Form_pg_authid) GETSTRUCT(tup);

			if (!pg_is_ascii(NameStr(authid->rolname)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing role name \"%s\" contains invalid characters",
								NameStr(authid->rolname)),
						 errhint("Consider ALTER ROLE ... RENAME TO ... using ASCII characters.")));
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_database */
		rel = table_open(DatabaseRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			Form_pg_database db = (Form_pg_database) GETSTRUCT(tup);

			if (!pg_is_ascii(NameStr(db->datname)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing database name \"%s\" contains invalid characters",
								NameStr(db->datname)),
						 errhint("Consider ALTER DATABASE ... RENAME TO ... using ASCII characters.")));

			/*
			 * Locale-related text fields requiring heap tuple deforming
			 * should already have been validated as pure ASCII, so we don't
			 * have to work harder here.
			 *
			 * XXX That's only true for the LC_ stuff; what about ICU, should
			 * it get the same treatment, or be checked here?
			 */
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_db_role_setting */
		rel = table_open(DbRoleSettingRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			Datum		setconfig;

			setconfig = heap_getattr(tup, Anum_pg_db_role_setting_setconfig,
									 RelationGetDescr(rel), &isnull);
			if (!isnull)
			{
				List	   *gucNames;
				List	   *gucValues;
				ListCell   *lc1;
				ListCell   *lc2;

				TransformGUCArray(DatumGetArrayTypeP(setconfig), &gucNames, &gucValues);
				forboth(lc1, gucNames, lc2, gucValues)
				{
					char	   *name = lfirst(lc1);
					char	   *value = lfirst(lc2);

					if (!pg_is_ascii(name) || !pg_is_ascii(value))
					{
						Datum		db_id;
						Datum		role_id;

						db_id = heap_getattr(tup, Anum_pg_db_role_setting_setdatabase,
											 RelationGetDescr(rel), &isnull);
						role_id = heap_getattr(tup, Anum_pg_db_role_setting_setrole,
											   RelationGetDescr(rel), &isnull);

						if (DatumGetObjectId(db_id) == InvalidOid)
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
									 errmsg("role \"%s\" has setting \"%s\" with value \"%s\" that contains invalid characters",
											GetUserNameFromId(DatumGetObjectId(role_id), false),
											name,
											value),
									 errhint("Consider ALTER ROLE ... SET ... TO ... using ASCII characters.")));
						else
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
									 errmsg("role \"%s\" has setting \"%s\" with value \"%s\" in database \"%s\" that contains invalid characters",
											GetUserNameFromId(DatumGetObjectId(role_id), false),
											name,
											value,
											get_database_name(DatumGetObjectId(db_id))),
									 errhint("Consider ALTER ROLE ... IN DATABASE ... SET ... TO ... using ASCII characters.")));
					}
					pfree(name);
					pfree(value);
				}
				list_free(gucNames);
				list_free(gucValues);
			}
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_parameter_acl */
		rel = table_open(ParameterAclRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			char	   *parname;

			parname = TextDatumGetCString(heap_getattr(tup, Anum_pg_parameter_acl_parname,
													   RelationGetDescr(rel), &isnull));

			/*
			 * This probably shouldn't happen as they are GUC names, so it's
			 * hard to suggest a useful hint.
			 */
			if (!pg_is_ascii(parname))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing ACL parameter name name \"%s\" contains invalid characters",
								parname)));
			pfree(parname);
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_replication_origin */
		rel = table_open(ReplicationOriginRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			char	   *s;

			s = TextDatumGetCString(heap_getattr(tup, Anum_pg_replication_origin_roname,
												 RelationGetDescr(rel), &isnull));
			if (!pg_is_ascii(s))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("replication origin \"%s\" contains invalid characters",
								s),
						 errhint("Consider recreating the replication origin using ASCII characters.")));
			pfree(s);
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_shdescription */
		rel = table_open(SharedDescriptionRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			char	   *s;

			s = TextDatumGetCString(heap_getattr(tup, Anum_pg_shdescription_description,
												 RelationGetDescr(rel), &isnull));
			if (!pg_is_ascii(s))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("comment \"%s\" on a shared database object contains invalid characters",
								s),
						 errhint("Consider COMMENT ON ... IS ... using ASCII characters.")));
			pfree(s);
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_shseclabel */
		rel = table_open(SharedSecLabelRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			char	   *s;

			s = TextDatumGetCString(heap_getattr(tup, Anum_pg_shseclabel_provider,
												 RelationGetDescr(rel), &isnull));
			if (!pg_is_ascii(s))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("security label provider name \"%s\" contains invalid characters",
								s),
						 errhint("This security label provider cannot be used with CLUSTER ENCODING set to ASCII.")));
			pfree(s);

			s = TextDatumGetCString(heap_getattr(tup, Anum_pg_shseclabel_label,
												 RelationGetDescr(rel), &isnull));
			if (!pg_is_ascii(s))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("a security label on a shared database object contains invalid characters"),
						 errhint("Security labels applied to shared database objects must be compatible with the current CLUSTER ENCODING.")));
			pfree(s);
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_subscription */
		rel = table_open(SubscriptionRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			bool		isnull;
			char	   *name;
			char	   *s;

			name = NameStr(*DatumGetName(heap_getattr(tup, Anum_pg_subscription_subname,
													  RelationGetDescr(rel), &isnull)));
			if (!pg_is_ascii(name))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing subscription name \"%s\" contains invalid characters",
								name),
						 errhint("Consider ALTER SUBSCRIPTION ... RENAME TO ... using ASCII characters.")));

			s = TextDatumGetCString(heap_getattr(tup, Anum_pg_subscription_subconninfo,
												 RelationGetDescr(rel), &isnull));
			if (!pg_is_ascii(s))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing subscription \"%s\" has connection string \"%s\" containing invalid characters",
								name, s),
						 errhint("Consider ALTER SUBSCRIPTION ... CONNECTION ... using ASCII characters.")));
			pfree(s);

			/*
			 * subsynccommit, subslotname and suborigin have their own
			 * validation that requires ASCII, so no check for now.
			 */

			/* XXX TODO check subpublications, a text[] */
		}
		systable_endscan(scan);
		table_close(rel, NoLock);

		/* pg_tablespace */
		rel = table_open(TableSpaceRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			Form_pg_tablespace ts = (Form_pg_tablespace) GETSTRUCT(tup);

			if (!pg_is_ascii(NameStr(ts->spcname)))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("existing tablespace name \"%s\" contains invalid characters",
								NameStr(ts->spcname)),
						 errhint("Consider ALTER TABLESPACE ... RENAME TO ... using ASCII characters.")));
		}
		systable_endscan(scan);
		table_close(rel, NoLock);
	}
	else
	{
		/* Make sure all databases are using this encoding. */
		rel = table_open(DatabaseRelationId, NoLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
		while ((tup = systable_getnext(scan)))
		{
			Form_pg_database db = (Form_pg_database) GETSTRUCT(tup);

			if (db->encoding != encoding)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("database \"%s\" has incompatible encoding %s",
								NameStr(db->datname),
								pg_encoding_to_char(db->encoding))));
		}
		systable_endscan(scan);
		table_close(rel, NoLock);
	}

	/* If we made it this far, we are allowed to change it. */
	SetClusterEncoding(encoding);
}

const char *
show_cluster_encoding(void)
{
	int			encoding;

	/* XXX locking? */
	encoding = GetClusterEncoding();
	if (encoding == CLUSTER_ENCODING_UNDEFINED)
		return "UNDEFINED";
	else if (encoding == CLUSTER_ENCODING_ASCII)
		return "ASCII";
	else
		return "DATABASE";
}
