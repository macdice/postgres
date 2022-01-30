/*-------------------------------------------------------------------------
 *
 * socket_table.c
 *	  Routines for dealing with extra per-socket state on Windows.
 *
 * Windows sockets can only safely be associate with one 'event', or they risk
 * losing FD_CLOSE events.  FD_CLOSE is also edge-triggered and not
 * resettable, so we need space for state to make it level-triggered.
 *
 * These functions are no-ops on Unix systems, except in assertion builds
 * where we do the minimum book keeping required to report failure to register
 * sockets appropriately.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/port/socket_table.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/hashfn.h"

typedef struct SocketTableEntry
{
	pgsocket	sock;
	int			status;
	ExtraSocketState *extra;
} SocketTableEntry;

#define SH_PREFIX		socktab
#define SH_ELEMENT_TYPE SocketTableEntry
#define SH_KEY_TYPE		pgsocket
#define SH_KEY			sock
#define SH_HASH_KEY(tb, key)	hash_bytes((uint8 *) &(key), sizeof(key))
#define SH_EQUAL(tb, a, b)		((a) == (b))
#define SH_SCOPE static inline
#define SH_NO_OOM
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

#if defined(WIN32) || defined(USE_ASSERT_CHECKING)
static socktab_hash *socket_table;
#endif

/*
 * Must be called exactly once for each socket before including it in code
 * that requires ExtraSocketState, such as WaitEventSet.  The socket must be
 * not already registered, and must be dropped before closing it.  Return NULL
 * on out-of-memory, if no_oom is true, otherwise raises errors.
 */
ExtraSocketState *
SocketTableAdd(pgsocket sock, bool no_oom)
{
#if defined(WIN32) || defined(USE_ASSERT_CHECKING)
	SocketTableEntry *ste;
	ExtraSocketState *ess;
	bool		found;

	/*
	 * Create a new SocketTableEntry object.  We can't use a pointer into the
	 * hash table because it would move around, and we want WaitEventSet to be
	 * able to hold pointers.
	 */
	if (!(ess = malloc(sizeof(*ess))))
		goto out_of_memory;
	memset(ess, 0, sizeof(*ess));

#ifdef WIN32
	/* Set up Windows kernel event for this socket. */
	ess->event_handle = WSACreateEvent();
	if (ess->event_handle == WSA_INVALID_EVENT)
		goto out_of_memory;
#endif

	/* Create socket_table on demand. */
	/* XXX dedicated memory context for debugging? */
	if (!socket_table &&
		(!(socket_table = socktab_create(TopMemoryContext, 128, NULL))))
		goto out_of_memory;

	/* Try to insert. */
	ste = socktab_insert(socket_table, sock, &found);
	if (!ste)
		goto out_of_memory;
	if (found)
	{
#ifdef WIN32
		WSACloseEvent(ess->event_handle);
#endif
		free(ess);
		elog(ERROR, "cannot register socket, already registered");
	}

	/* Success. */
	ste->extra = ess;
	return ess;

 out_of_memory:
	if (ess)
	{
#ifdef WIN32
		if (ess->event_handle != WSA_INVALID_EVENT)
			WSACloseEvent(ess->event_handle);
#endif
		free(ess);
	}
	if (!no_oom)
		elog(ERROR, "out of memory");
	return NULL;
#else
	return NULL;
#endif
}

/*
 * Unregister a socket that was registered.  This must be done before closing
 * the socket.
 */
void
SocketTableDrop(pgsocket sock)
{
#if defined(WIN32) || defined(USE_ASSERT_CHECKING)
	SocketTableEntry *ste = NULL;
	ExtraSocketState *ess;

	if (socket_table)
		ste = socktab_lookup(socket_table, sock);
	if (!ste)
		elog(ERROR, "cannot unregister socket that is not registered");
	ess = ste->extra;

#ifdef WIN32
	WSACloseEvent(ess->event_handle);
#endif

	free(ess);
	socktab_delete_item(socket_table, ste);
#endif
}

ExtraSocketState *
SocketTableGet(pgsocket sock)
{
#if defined(WIN32) || defined(USE_ASSERT_CHECKING)
	SocketTableEntry *ste = NULL;

	if (socket_table)
		ste = socktab_lookup(socket_table, sock);
	if (!ste)
		elog(ERROR, "socket is not registered");
	return ste->extra;
#else
	return NULL;
#endif
}
