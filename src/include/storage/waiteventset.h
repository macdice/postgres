/*-------------------------------------------------------------------------
 *
 * waiteventset.h
 *		ppoll() / pselect() like interface for waiting for events
 *
 * WaitEventSets allow to wait for latches being set and additional events -
 * postmaster dying and socket readiness of several sockets currently - at the
 * same time.  On many platforms using a long lived event set is more
 * efficient than using WaitLatch or WaitLatchOrSocket.
 *
 * WaitEventSetWait includes a provision for timeouts (which should be avoided
 * when possible, as they incur extra overhead) and a provision for postmaster
 * child processes to wake up immediately on postmaster death.  See
 * storage/ipc/waiteventset.c for detailed specifications for the exported
 * functions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/waiteventset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WAITEVENTSET_H
#define WAITEVENTSET_H

#include "utils/resowner.h"

/*
 * Bitmasks for events that may wake-up WaitLatch(), WaitLatchOrSocket(), or
 * WaitEventSetWait().
 */
#define WL_LATCH_SET		 (1 << 0)
#define WL_SOCKET_READABLE	 (1 << 1)
#define WL_SOCKET_WRITEABLE  (1 << 2)
#define WL_TIMEOUT			 (1 << 3)	/* not for WaitEventSetWait() */
#define WL_POSTMASTER_DEATH  (1 << 4)
#define WL_EXIT_ON_PM_DEATH	 (1 << 5)
#ifdef WIN32
#define WL_SOCKET_CONNECTED  (1 << 6)
#else
/* avoid having to deal with case on platforms not requiring it */
#define WL_SOCKET_CONNECTED  WL_SOCKET_WRITEABLE
#endif
#define WL_SOCKET_CLOSED 	 (1 << 7)
#ifdef WIN32
#define WL_SOCKET_ACCEPT	 (1 << 8)
#else
/* avoid having to deal with case on platforms not requiring it */
#define WL_SOCKET_ACCEPT	WL_SOCKET_READABLE
#endif
#define WL_SOCKET_MASK		(WL_SOCKET_READABLE | \
							 WL_SOCKET_WRITEABLE | \
							 WL_SOCKET_CONNECTED | \
							 WL_SOCKET_ACCEPT | \
							 WL_SOCKET_CLOSED)

/* Flags for internal usage (defined here only to avoid collisions). */
#define WL_INTERNAL_FLAG1	(1 << 31)
#define WL_WAKE				(1 << 30)

/* Type of waitable object. */
typedef enum
{
	WL_TYPE_INVALID,
	WL_TYPE_POSTMASTER,			/* id not used */
	WL_TYPE_LATCH,				/* id is a pointer to Latch */
	WL_TYPE_SOCKET,				/* id is a socket descriptor */
	
	WL_TYPE_WAKE,				/* internal usage only */
	
	WL_TYPE_LAST = WL_TYPE_WAKE,
} WaitEventType;

/* Identifier for a waitable object. */
typedef intptr_t WaitEventId;

typedef struct WaitEvent
{
	WaitEventType	id_type;
	WaitEventId	id;
	uint32		events;			/* triggered events */
	void	   *user_data;		/* pointer provided when adding */
	int			pos;			/* internal position number */
#ifdef WIN32
	bool		reset;			/* Is reset of the event required? */
#endif
} WaitEvent;

#define WL_HANDLE_SIZE 8

typedef struct WaitEventSetHandle
{
	alignas(MAXIMUM_ALIGNOF) char opaque[WL_HANDLE_SIZE];
} WaitEventSetHandle;

/* forward declarations to avoid exposing waiteventset.c implementation details */
typedef struct WaitEventSet WaitEventSet;

struct Latch;

/*
 * prototypes for functions in waiteventset.c
 */
extern void InitializeWaitEventSupport(void);

extern WaitEventSet *CreatePhysicalWaitEventSet(ResourceOwner resowner,
												int nevents);
extern WaitEventSet *CreateLogicalWaitEventSet(ResourceOwner resowner,
											   WaitEventSet *physical,
											   int nevents);

extern WaitEventSet *CreateWaitEventSet(ResourceOwner resowner, int nevents);

extern void ReserveWaitEventSetSpace(WaitEventSet *set, int nevents);
extern void FreeWaitEventSet(WaitEventSet *set);
extern void FreeWaitEventSetAfterFork(WaitEventSet *set);

/* Generic operations that work for all types. */
extern bool AddWaitEventSetObject(WaitEventSet *set,
								  WaitEventType id_type,
								  WaitEventId id,
								  uint32 event_mask,
								  void *user_data);
extern void ModifyWaitEventSetObject(WaitEventSet *set,
									 WaitEventType id_type,
									 WaitEventId id,
									 uint32 event_mask);
extern bool DeleteWaitEventSetObject(WaitEventSet *set,
									 WaitEventType id_type,
									 WaitEventId id);
extern int	DeleteWaitEventSetObjectType(WaitEventSet *set,
										 WaitEventType id_type);

/* Typed variants for latches. */
extern void AddWaitEventSetLatch(WaitEventSet *set, struct Latch *latch);
extern void DeleteWaitEventSetLatch(WaitEventSet *set, struct Latch *latch);
extern void DeleteWaitEventSetLatches(WaitEventSet *set);

/* Typed variants for sockets. */
extern void AddWaitEventSetSocket(WaitEventSet *set,
								  pgsocket socket,
								  int event_mask,
								  void *user_data);
extern void ModifyWaitEventSetSocket(WaitEventSet *set,
									 pgsocket socket,
									 uint32 event_mask);
extern void DeleteWaitEventSetSocket(WaitEventSet *set,
									 pgsocket socket);

/* Typed variants for postmaster. */
extern void AddWaitEventSetPostmaster(WaitEventSet *set, uint32 event_mask);
extern void ModifyWaitEventSetPostmaster(WaitEventSet *set, uint32 event_mask);

extern int	WaitEventSetWait(WaitEventSet *set, long timeout,
							 WaitEvent *occurred_events, int nevents,
							 uint32 wait_event_info);
extern int	GetNumRegisteredWaitEvents(WaitEventSet *set);
extern bool WaitEventSetCanReportClosed(void);

extern void WakeWaitEventSet(WaitEventSetHandle *handle);

#ifndef WIN32
extern void WakeupMyProc(void);
extern void WakeupOtherProc(int pid);
#endif


#endif							/* WAITEVENTSET_H */
