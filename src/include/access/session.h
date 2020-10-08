/*-------------------------------------------------------------------------
 *
 * session.h
 *	  Encapsulation of user session.
 *
 * Copyright (c) 2017-2020, PostgreSQL Global Development Group
 *
 * src/include/access/session.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SESSION_H
#define SESSION_H

#include "lib/dshash.h"
#include "storage/lwlock.h"

struct SharedComboCidLock;
struct SharedRecordTypmodRegistry;

/*
 * Part of the session object that is of fixed size in shared memory.
 */
typedef struct SessionFixed
{
	/* State managed by combocid.c. */
	LWLock		shared_combocid_lock;
	dsa_pointer	shared_combocid_registry_dsa;
	int			shared_combocid_change;
} SessionFixed;

/*
 * A struct encapsulating some elements of a user's session.  For now this
 * manages state that applies to parallel query, but in principle it could
 * include other things that are currently global variables.
 */
typedef struct Session
{
	dsm_segment *segment;		/* The session-scoped DSM segment. */
	dsa_area   *area;			/* The session-scoped DSA area. */

	SessionFixed *fixed;

	/* State managed by combocid.c. */
	struct SharedComboCidRegistry *shared_combocid_registry;
	int			shared_combocid_change;

	/* State managed by typcache.c. */
	struct SharedRecordTypmodRegistry *shared_typmod_registry;
	dshash_table *shared_record_table;
	dshash_table *shared_typmod_table;
} Session;

extern void InitializeSession(void);
extern dsm_handle GetSessionDsmHandle(void);
extern void AttachSession(dsm_handle handle);
extern void DetachSession(void);

/* The current session, or NULL for none. */
extern Session *CurrentSession;

#endif							/* SESSION_H */
