/*-------------------------------------------------------------------------
 *
 * appendinfo.h
 *	  Routines for mapping expressions between append rel parent(s) and
 *	  children
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/appendinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPENDINFO_H
#define APPENDINFO_H

#include "nodes/pathnodes.h"
#include "utils/relcache.h"

extern AppendRelInfo *make_append_rel_info(Relation parentrel,
										   Relation childrel,
										   Index parentRTindex, Index childRTindex);
extern Node *adjust_appendrel_attrs(PlannerInfo *root, Node *node,
									int nappinfos, AppendRelInfo **appinfos);
extern Node *adjust_appendrel_attrs_multilevel(PlannerInfo *root, Node *node,
											   RelOptInfo *childrel,
											   RelOptInfo *parentrel);
extern Relids adjust_child_relids(Relids relids, int nappinfos,
								  AppendRelInfo **appinfos);
extern Relids adjust_child_relids_multilevel(PlannerInfo *root, Relids relids,
											 RelOptInfo *childrel,
											 RelOptInfo *parentrel);
extern List *adjust_inherited_attnums(List *attnums, AppendRelInfo *context);
extern List *adjust_inherited_attnums_multilevel(PlannerInfo *root,
												 List *attnums,
												 Index child_relid,
												 Index top_parent_relid);
extern void get_translated_update_targetlist(PlannerInfo *root, Index relid,
											 List **processed_tlist,
											 List **update_colnos);
extern AppendRelInfo **find_appinfos_by_relids_in_place(AppendRelInfo **appinfos,
														PlannerInfo *root,
														Relids relids,
														int *nappinfos);
extern void add_row_identity_var(PlannerInfo *root, Var *orig_var,
								 Index rtindex, const char *rowid_name);
extern void add_row_identity_columns(PlannerInfo *root, Index rtindex,
									 RangeTblEntry *target_rte,
									 Relation target_relation);
extern void distribute_row_identity_vars(PlannerInfo *root);

typedef struct AppendRelInfoBuffer
{
	AppendRelInfo **appinfos;
	int			capacity;
	int			want;
} AppendRelInfoBuffer;

/*
 * The following are written as a macros so they can use a stack buffer to
 * place the results in the calling stack frame if possible.  A stack buffer
 * must be declared in the current scope to use them.
 */

#define find_appinfos_by_relids(root, relids, nappinfos)				\
	find_appinfos_by_relids_in_place(pg_stack_alloc_array(AppendRelInfo *, \
															  bms_num_members(relids)), \
									 (root),							\
									 (relids),							\
									 (nappinfos))
#define free_appinfos(appinfos)					\
	if (appinfos)								\
		pg_stack_free(appinfos)

#define find_appinfos_by_relids_with_buffer(buffer, root, relids, nappinfos) \
	((buffer)->want = bms_num_members(relids),							\
	 (buffer)->appinfos = ((buffer)->capacity >= (buffer)->want ?		\
						   (buffer)->appinfos :							\
						   ((buffer)->capacity = (buffer)->want,		\
							pg_stack_alloc_array(AppendRelInfo *,	\
													 (buffer)->capacity))), \
	 find_appinfos_by_relids_in_place((buffer)->appinfos,				\
									  (root),							\
									  (relids),							\
									  (nappinfos)))

#define free_appinfos_buffer(buffer)				\
	if ((buffer)->appinfos)							\
	{												\
		pg_stack_free((buffer)->appinfos);		\
		(buffer)->appinfos = NULL;					\
		(buffer)->capacity = 0;						\
	}

#endif							/* APPENDINFO_H */
