/*-------------------------------------------------------------------------
 *
 * pg_futex.h
 *
 * Futex abstraction.  Initially supported only on macOS, but it could be
 * extended to other systems.  A wrapper struct and initialization function are
 * used to reserve the option of providing a fallback implementation.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_futex.h
 *
 *-------------------------------------------------------------------------/
 */
#ifndef PG_FUTEX_H
#define PG_FUTEX_H

#ifdef FRONTEND
#error "not for use in frontend code"
#endif

#if HAVE_DECL_OS_SYNC_WAIT_ON_ADDRESS
#include <os/os_sync_wait_on_address.h>
#define HAVE_PG_FUTEX_U32
#endif

#ifdef HAVE_PG_FUTEX_U32

#include "port/atomics.h"

#include <time.h>

typedef struct pg_futex_u32
{
	pg_atomic_uint32 value;
} pg_futex_u32;

static inline void
pg_futex_init_u32(pg_futex_u32 *futex, uint32 value)
{
	pg_atomic_init_u32(&futex->value, value);
}

/*
 * Return 0 immediately if futex->value is not equal to the expected value, and
 * otherwise wait to be woken up explicitly by a thread that has changed the
 * value, and then return 0.  Return -1 and set errno on error.  ETIMEDOUT
 * indicates that the optional timeout has been reached.
 *
 * Note for future implementations: not all systems distinguish between value
 * check failure and being woken up, so this function returns 0 in both cases.
 */
static inline int
pg_futex_wait_u32(pg_futex_u32 *futex, uint32 expected, struct timespec *timeout)
{
	/*
	 * Our atomic 32 bit integers are just wrapped integers, so it is safe for
	 * the kernel to read them when it performs its value check.
	 */
	StaticAssertStmt(sizeof(futex->value) == sizeof(expected), "unexpected size");

#if HAVE_DECL_OS_SYNC_WAIT_ON_ADDRESS
	if (timeout)
	{
		uint64		timeout_ns = timeout->tv_sec * 1000000000 + timeout->tv_nsec;

		if (os_sync_wait_on_address_with_timeout(&futex->value,
												 expected,
												 sizeof(futex->value),
												 OS_SYNC_WAIT_ON_ADDRESS_SHARED,
												 OS_CLOCK_MACH_ABSOLUTE_TIME,
												 timeout_ns) >= 0)
			return 0;
	}
	else
	{
		if (os_sync_wait_on_address(&futex->value,
									expected,
									sizeof(futex->value),
									OS_SYNC_WAIT_ON_ADDRESS_SHARED) >= 0)
			return 0;
	}
	return -1;
#else
#error "futexes not implemented for this platform"
#endif
}

/*
 * Wake at least a given number of waiters.
 *
 * Note for future implementations: not all systems report how many were woken,
 * so this function hides that by returning 0 for success.
 */
static inline int
pg_futex_wake_u32(pg_futex_u32 *futex, int nwaiters)
{
#if HAVE_DECL_OS_SYNC_WAIT_ON_ADDRESS
	int			rc;

	if (nwaiters == 1)
		rc = os_sync_wake_by_address_any(&futex->value,
										 sizeof(futex->value),
										 OS_SYNC_WAKE_BY_ADDRESS_SHARED);
	else
		rc = os_sync_wake_by_address_all(&futex->value,
										 sizeof(futex->value),
										 OS_SYNC_WAKE_BY_ADDRESS_SHARED);
	if (rc < 0 && errno == ENOENT)
		rc = 0;					/* no waiters */
	return rc;
#else
#error "futexes not implemented for this platform"
#endif
}

#endif							/* HAVE_PG_FUTEX_U32 */

#endif							/* PG_FUTEX_H */
