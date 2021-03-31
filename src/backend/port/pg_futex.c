/*
 * A very thin portable abstraction over futex APIs.
 */

#include "postgres.h"

#include "port/pg_futex.h"

#if defined(HAVE_PG_FUTEX_T)

volatile pg_atomic_futex_t *pg_futex_interruptible;
volatile sig_atomic_t pg_futex_interrupt_op;

#endif

#if defined(HAVE_LINUX_FUTEX_H)

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

int
pg_futex_wait(volatile pg_atomic_futex_t *fut,
			  pg_futex_t value,
			  struct timespec *timeout)
{
	return syscall(SYS_futex, fut, FUTEX_WAIT, value, timeout, 0, 0);
}

int
pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters)
{
	return syscall(SYS_futex, fut, FUTEX_WAKE, nwaiters, NULL, 0, 0);
}

#endif

#if defined(HAVE_SYS_FUTEX_H)

/* https://man.openbsd.org/futex */

#include <sys/time.h>
#include <sys/futex.h>

int
pg_futex_wait(volatile pg_atomic_futex_t *fut,
			  pg_futex_t value,
			  struct timespec *timeout)
{
	int rc;

	rc = futex((volatile void *) fut, FUTEX_WAIT, value, timeout, 0);
	if (rc == 0)
		return 0;
	if (rc == ECANCELED)
		rc = EINTR;
	errno = rc;

	return -1;
}

int
pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters)
{
	int rc;

	rc = futex((volatile void *) fut, FUTEX_WAKE, nwaiters, 0, 0);
	if (rc == 0)
		return 0;
	errno = rc;

	return -1;
}

#endif

#if defined(HAVE_SYS_UMTX_H)

#include <sys/types.h>
#include <sys/umtx.h>

int
pg_futex_wait(volatile pg_atomic_futex_t *fut,
			 pg_futex_t value,
			 struct timespec *timeout)
{
	return _umtx_op((void *) fut, UMTX_OP_WAIT, value, 0, timeout);
}

int
pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters)
{
	return _umtx_op((void *) fut, UMTX_OP_WAKE, nwaiters, 0, 0);
}

#endif

#if defined(HAVE___ULOCK_WAIT)

#include <stdint.h>

/*
 * This interface is undocumented, but provided by libSystem.dylib since
 * xnu-3789.1.32 (macOS 10.12, 2016) and is used by eg libc++.
 *
 * https://github.com/apple/darwin-xnu/blob/main/bsd/kern/sys_ulock.c
 * https://github.com/apple/darwin-xnu/blob/main/bsd/sys/ulock.h
 */

#define UL_COMPARE_AND_WAIT64_SHARED    6
#define ULF_WAKE_ALL                    0x00000100
#define ULF_WAKE_THREAD                 0x00000200
extern int __ulock_wait(uint32_t operation,
						void *addr,
						uint64_t value,
						uint32_t timeout);
extern int __ulock_wake(uint32_t operation,
						void *addr,
						uint64_t wake_value);

int
pg_futex_wait(volatile pg_atomic_futex_t *fut,
			 pg_futex_t value,
			 struct timespec *timeout)
{
	uint32_t us;

	if (timeout == NULL)
		us = 0;
	else
		us = timeout->tv_sec * 1000000 + timeout->tv_nsec / 1000;

	return __ulock_wait(UL_COMPARE_AND_WAIT64_SHARED, (void *) fut, value, us);
}

int
pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters)
{
	int flags = nwaiters == 1 ? ULF_WAKE_THREAD : ULF_WAKE_ALL;

	return __ulock_wake(UL_COMPARE_AND_WAIT64_SHARED | flags, (void *) fut, 0);
}

#endif
