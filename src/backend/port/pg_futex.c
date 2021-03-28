/*
 * A very thin portable abstraction over futex APIs.
 */

#include "postgres.h"

#include "port/pg_futex.h"

#if defined(HAVE_PG_FUTEX_T)

volatile sig_atomic_t please_increment_futex;
volatile pg_atomic_futex_t *futex_to_increment;

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
