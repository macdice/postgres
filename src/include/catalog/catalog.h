/*-------------------------------------------------------------------------
 *
 * catalog.h
 *	  prototypes for functions in backend/catalog/catalog.c
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CATALOG_H
#define CATALOG_H

#include "catalog/pg_class.h"
#include "utils/relcache.h"


extern bool IsSystemRelation(Relation relation);
extern bool IsToastRelation(Relation relation);
extern bool IsCatalogRelation(Relation relation);
extern bool IsInplaceUpdateRelation(Relation relation);

extern bool IsSystemClass(Oid relid, Form_pg_class reltuple);
extern bool IsToastClass(Form_pg_class reltuple);

extern bool IsCatalogRelationOid(Oid relid);
extern bool IsInplaceUpdateOid(Oid relid);

extern bool IsCatalogNamespace(Oid namespaceId);
extern bool IsToastNamespace(Oid namespaceId);

extern bool IsReservedName(const char *name);

extern bool IsSharedRelation(Oid relationId);

extern bool IsPinnedObject(Oid classId, Oid objectId);

extern Oid	GetNewOidWithIndex(Relation relation, Oid indexId,
							   AttrNumber oidcolumn);
extern RelFileNumber GetNewRelFileNumber(Oid reltablespace,
										 Relation pg_class,
										 char relpersistence);
extern bool StringIsValidInClusterEncoding(const char *s, int cluster_encoding);
extern bool StringIsValidInCurrentClusterEncoding(const char *s);
extern void ValidateSharedCatalogString(Relation rel, const char *s);
extern void AlterSystemSetClusterEncoding(const char *encoding_name);
extern const char *show_cluster_encoding(void);

#endif							/* CATALOG_H */
