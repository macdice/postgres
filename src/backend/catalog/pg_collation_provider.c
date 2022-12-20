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
#if 0
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_pg_collation];
	bool		nulls[Natts_pg_collation];
	NameData	name_name;
	Oid			oid;
	ObjectAddress myself,
				referenced;
#endif

	
	Assert(collproname);
	Assert(collprotype == COLLPROVIDERTYPE_LIBC ||
		   collprotype == COLLPROVIDERTYPE_ICU);
	Assert(collprodata);

	/*
	 * Check for existing provider with the same name, to try to provide a
	 * nice error message.  The unique index provides a backstop against race
	 * conditions.
	 */
	return InvalidOid;
}
