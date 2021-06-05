/*-------------------------------------------------------------------------
 *
 * interrupt.h
 *	  Interrupt handling routines.
 *
 * "Interrupts" are a set of flags that represent conditions that should be
 * handled at a later time.  They are roughly analogous to Unix signals,
 * except that they are handled cooperatively by checking for them at many
 * points in the code.
 *
 * Interrupt flags can be "raised" synchronously by code that wants to defer
 * an action (for example: INTERRUPT_CONNECTION_LOST), or asynchronously by
 * timer signal handlers (for example: INTERRUPT_IDLE_SESSION_TIMEOUT), other
 * signal handlers (for example: INTERRUPT_QUERY_CANCEL) or "sent" by other
 * backends setting them directly.
 *
 * In the case of asynchronous interrupts, the target backend's latch is also
 * set, to make sure that the backend wakes from latch sleeps.  Well behaved
 * backend code performs CHECK_FOR_INTERRUPTS() periodically in long
 * computations, and should never sleep using mechanisms other than the latch
 * wait mechanism (except for bounded short periods, eg LWLock waits), so they
 * should react in good time.
 *
 * The "standard" set of interrupts is handled by CHECK_FOR_INTERRUPTS(), and
 * consists of tasks that are safe to perform at most times.  They can be
 * suppressed by HOLD_INTERRUPTS()/RESUME_INTERRUPTS().
 *
 * Other special interrupts are checked for explicitly.

 * Responses to signals that are translated to interrupts are fairly varied
 * and many types of backends have their own implementations, but we provide a
 * few generic signal handlers and interrupt checks here to facilitate code
 * reuse.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/postmaster/interrupt.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERRUPT_H
#define INTERRUPT_H

#include "port/atomics.h"
#include "storage/procnumber.h"

#include <signal.h>

extern PGDLLIMPORT volatile sig_atomic_t ConfigReloadPending;
extern PGDLLIMPORT volatile sig_atomic_t ShutdownRequestPending;

extern PGDLLIMPORT uint32 InterruptHoldoffCount;
extern PGDLLIMPORT uint32 QueryCancelHoldoffCount;

extern PGDLLIMPORT pg_atomic_uint32 *MyPendingInterrupts;

typedef enum
{
	/* Sent by other backends. */
	INTERRUPT_SINVAL_CATCHUP,
	INTERRUPT_NOTIFY,			/* listen/notify interrupt */
	INTERRUPT_PARALLEL_APPLY_MESSAGE,	/* message from cooperating parallel
										 * backend */
	INTERRUPT_PARALLEL_MESSAGE, /* message from cooperating parallel backend */
	INTERRUPT_WALSND_INIT_STOPPING, /* ask walsenders to prepare for shutdown  */
	INTERRUPT_BARRIER,			/* global barrier interrupt  */
	INTERRUPT_LOG_MEMORY_CONTEXT,	/* ask backend to log the memory contexts */

	/* Raised by timers. */
	INTERRUPT_TRANSACTION_TIMEOUT,
	INTERRUPT_IDLE_SESSION_TIMEOUT,
	INTERRUPT_IDLE_TRANSACTION_TIMEOUT,
	INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT,
	INTERRUPT_CHECK_CONNECTION_TIMEOUT,

	/* Raised by signal handlers (usually sent from the postmaster). */
	INTERRUPT_QUERY_CANCEL,
	INTERRUPT_DIE,

	/* Raised synchronously. */
	INTERRUPT_CONNECTION_LOST,

	/* Sent by startup process. */
	INTERRUPT_RECOVERY_CONFLICT_FIRST,
	INTERRUPT_RECOVERY_CONFLICT_DATABASE = INTERRUPT_RECOVERY_CONFLICT_FIRST,
	INTERRUPT_RECOVERY_CONFLICT_TABLESPACE,
	INTERRUPT_RECOVERY_CONFLICT_LOCK,
	INTERRUPT_RECOVERY_CONFLICT_SNAPSHOT,
	INTERRUPT_RECOVERY_CONFLICT_BUFFERPIN,
	INTERRUPT_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
	INTERRUPT_RECOVERY_CONFLICT_LOGICALSLOT,
	INTERRUPT_RECOVERY_CONFLICT_LAST = INTERRUPT_RECOVERY_CONFLICT_LOGICALSLOT
} InterruptType;

/*
 * CHECK_FOR_INTERRUPTS() ignores special interrupts that are only handled when
 * the backend is idle.
 */
#define INTERRUPT_IDLE_MASK                       \
	((1 << INTERRUPT_SINVAL_CATCHUP) |            \
	 (1 << INTERRUPT_NOTIFY))
#define INTERRUPT_REGULAR_MASK ~INTERRUPT_IDLE_MASK

/* Test whether a regular interrupt is pending */
#ifndef WIN32
#define INTERRUPTS_PENDING_CONDITION() \
	unlikely((pg_atomic_read_u32(MyPendingInterrupts) & INTERRUPT_REGULAR_MASK) != 0)
#else
#define INTERRUPTS_PENDING_CONDITION() \
	(unlikely(UNBLOCKED_SIGNAL_QUEUE()) ? pgwin32_dispatch_queued_signals() : 0, \
	 unlikely((pg_atomic_read_u32(MyPendingInterrupts) & INTERRUPT_REGULAR_MASK) != 0))
#endif

/*
 * Service regular interrupt, if one is pending and it's safe to service it
 * now.
 */
#define CHECK_FOR_INTERRUPTS() \
do { \
	if (INTERRUPTS_PENDING_CONDITION()) \
		ProcessInterrupts(); \
} while(0)

/* Is ProcessInterrupts() guaranteed to clear regular interrupts? */
#define INTERRUPTS_CAN_BE_PROCESSED() \
	(InterruptHoldoffCount == 0 && CritSectionCount == 0 && \
	 QueryCancelHoldoffCount == 0)

#define HOLD_INTERRUPTS()  (InterruptHoldoffCount++)

#define RESUME_INTERRUPTS() \
do { \
	Assert(InterruptHoldoffCount > 0); \
	InterruptHoldoffCount--; \
} while(0)

#define HOLD_CANCEL_INTERRUPTS()  (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
	Assert(QueryCancelHoldoffCount > 0); \
	QueryCancelHoldoffCount--; \
} while(0)

/*
 * Test an interrupt flag.
 */
static inline bool
InterruptIsPending(InterruptType reason)
{
	return (pg_atomic_read_u32(MyPendingInterrupts) & (1 << reason)) != 0;
}

/*
 * Clear an interrupt flag.
 */
static inline void
ClearInterrupt(InterruptType reason)
{
	pg_atomic_fetch_and_u32(MyPendingInterrupts, ~(1 << reason));
}

/*
 * Test and clear an interrupt flag.
 */
static inline bool
ConsumeInterrupt(InterruptType reason)
{
	if (likely(!InterruptIsPending(reason)))
		return false;

	ClearInterrupt(reason);

	return true;
}

extern void RaiseInterrupt(InterruptType reason);
extern void SendInterrupt(InterruptType reason, ProcNumber pgprocno);
extern void SwitchToLocalInterrupts(void);
extern void SwitchToSharedInterrupts(void);

/* in tcop/postgres.c */
extern void ProcessInterrupts(void);

/*
 * A handler for INTERRUPT_BARRIER, and the reload/exit/shutdown flags set by
 * the signal handlers below.
 */
extern void HandleMainLoopInterrupts(void);

/* Common signal handlers. */
extern void SignalHandlerForConfigReload(SIGNAL_ARGS);
extern void SignalHandlerForCrashExit(SIGNAL_ARGS);
extern void SignalHandlerForShutdownRequest(SIGNAL_ARGS);

#endif
