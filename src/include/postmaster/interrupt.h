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
	INTERRUPT_NOTIFY,	/* listen/notify interrupt */
	INTERRUPT_PARALLEL_APPLY_MESSAGE,	/* message from cooperating parallel backend */
	INTERRUPT_PARALLEL_MESSAGE,	/* message from cooperating parallel backend */
	INTERRUPT_WALSND_INIT_STOPPING,	/* ask walsenders to prepare for shutdown  */
	INTERRUPT_BARRIER,			/* global barrier interrupt  */
	INTERRUPT_LOG_MEMORY_CONTEXT, /* ask backend to log the memory contexts */

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
 * The set of "standard" interrupts that CHECK_FOR_INTERRUPTS() and
 * ProcessInterrupts() handle.  These perform work that is safe to run whenever
 * interrupts are not "held".  Other kinds of interrupts are only handled at
 * more restricted times.
 */
#define INTERRUPT_STANDARD_MASK							   \
	((1 << INTERRUPT_DIE) |								   \
	 (1 << INTERRUPT_CHECK_CONNECTION_TIMEOUT) |		   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_DATABASE) |		   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_TABLESPACE) |	   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_LOCK) |			   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_SNAPSHOT) |		   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_BUFFERPIN) |		   \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_STARTUP_DEADLOCK) | \
	 (1 << INTERRUPT_RECOVERY_CONFLICT_LOGICALSLOT) |	   \
	 (1 << INTERRUPT_CONNECTION_LOST) |					   \
	 (1 << INTERRUPT_QUERY_CANCEL) |					   \
	 (1 << INTERRUPT_TRANSACTION_TIMEOUT) |				   \
	 (1 << INTERRUPT_IDLE_TRANSACTION_TIMEOUT) |		   \
	 (1 << INTERRUPT_IDLE_SESSION_TIMEOUT) |			   \
	 (1 << INTERRUPT_BARRIER) |							   \
	 (1 << INTERRUPT_PARALLEL_MESSAGE) |				   \
	 (1 << INTERRUPT_WALSND_INIT_STOPPING) |			   \
	 (1 << INTERRUPT_LOG_MEMORY_CONTEXT))

/* Test whether an interrupt is pending */
#define INTERRUPTS_PENDING_CONDITION() \
	unlikely((pg_atomic_read_u32(MyPendingInterrupts) & INTERRUPT_STANDARD_MASK) != 0)

/* Service interrupt, if one is pending and it's safe to service it now */
#define CHECK_FOR_INTERRUPTS() \
do { \
	if (INTERRUPTS_PENDING_CONDITION()) \
		ProcessInterrupts(); \
} while(0)

/* Is ProcessInterrupts() guaranteed to clear standard interrupts? */
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
extern void SendInterrupt(InterruptType reason, int pgprocno);
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
