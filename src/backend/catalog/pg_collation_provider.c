/*-------------------------------------------------------------------------
 *
 * pg_collation_provider.c
 *	  routines to support manipulation of the pg_collation_provider
 *	  relation
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_collation_provider.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_provider.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CollationProviderCreate
 *
 * Add a new tuple to pg_collation_provider.
 */
Oid
CollationProviderCreate(const char *collproname,
						char collprotype,
						const char *collprodata,
						bool if_not_exists,
						bool quiet)
{
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_pg_collation];
	bool		nulls[Natts_pg_collation];
	NameData	name_name;
	Oid			oid;
	ObjectAddress myself,
				referenced;

	
	Assert(collproname);
	Assert(collprotype == COLLPROVIDERTYPE_LIBC ||
		   collprotype == COLLPROVIDERTYPE_ICU);
	Assert(collprodata);

	/*
	 * Check for existing provider with the same name, to try to provide a
	 * nice error message.  The unique index provides a backstop against race
	 * conditions.
	 */
	oid = GetSysCacheOid1(COLLPRONAME,
						  Anum_pg_collation_provider_oid,
						  PointerGetDatum(collproname));
	if (OidIsValid(oid))
	{
		if (quiet)
			return InvalidOid;
		else if (if_not_exists)
		{
			/* OK to skip */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("collation provider \"%s\" already exists, skipping",
							collproname)));
			return InvalidOid;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("collation provider \"%s\" already exists",
							collproname)));
	}

	/* XXX this lock is too strong */
	rel = table_open(CollationProviderRelationId, ShareRowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	memset(nulls, 0, sizeof(nulls));
	namestrcpy(&name_name, collproname);
	oid = GetNewOidWithIndex(rel, CollationProviderIndexId,
							 Anum_pg_collation_provider_oid);
	values[Anum_pg_collation_provider_oid - 1] = ObjectIdGetDatum(oid);
	values[Anum_pg_collation_provider_collproname - 1] = NameGetDatum(&name_name);
	values[Anum_pg_collation_provider_collprotype - 1] = CharGetDatum(collprotype);
	values[Anum_pg_collation_provider_collprodata - 1] = CStringGetTextDatum(collprodata);
	tup = heap_form_tuple(tupDesc, values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);
	table_close(rel, NoLock);
	
	return InvalidOid;
}
