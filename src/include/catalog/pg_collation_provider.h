/*-------------------------------------------------------------------------
 *
 * pg_collation_provider.h
 *	  definition of the "collation provider" system catalog
 *	  (pg_collation_provider)
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_collation_provider.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_COLLATION_PROVIDER_H
#define PG_COLLATION_PROVIDER_H

#include "catalog/genbki.h"
#include "catalog/pg_collation_provider_d.h"

/* ----------------
 *		pg_collation_provider definition.  cpp turns this into
 *		typedef struct FormData_pg_collation_provider
 * ----------------
 */
CATALOG(pg_collation_provider,8888,CollationProviderRelationId)
{
	Oid			oid;			/* oid */
	NameData	collproname;	/* collation provider name */
	char		collprotype;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		collprodata BKI_DEFAULT(_null_);
#endif
} FormData_pg_collation_provider;

/* ----------------
 *		Form_pg_collation_provider corresponds to a pointer to a row with
 *		the format of pg_collation_provider relation.
 * ----------------
 */
typedef FormData_pg_collation_provider *Form_pg_collation_provider;

DECLARE_TOAST(pg_collation_provider, 8886, 8887);

DECLARE_UNIQUE_INDEX_PKEY(pg_collation_provider_oid_index, 8889, CollationProviderOidIndexId, on pg_collation_provider using btree(oid oid_ops));

#ifdef EXPOSE_TO_CLIENT_CODE

#define COLLPROVIDERTYPE_LIBC		'c'
#define COLLPROVIDERTYPE_ICU		'i'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_COLLATION_PROVIDkER_H */
