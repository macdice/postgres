/*
 * Minimal wrapper over futex APIs.
 */

#ifndef PG_FUTEX_H
#define PG_FUTEX_H

#if defined(HAVE_LINUX_FUTEX_H)

/* https://man7.org/linux/man-pages/man2/futex.2.html */

#include <linux/futex.h>
#include <sys/syscall.h>

#elif defined(HAVE_SYS_FUTEX_H)

/* https://man.openbsd.org/futex, since OpenBSD 6.2. */

#include <sys/time.h>
#include <sys/futex.h>

#elif defined(HAVE_SYS_UMTX_H)

/* https://www.freebsd.org/cgi/man.cgi?query=_umtx_op */

#include <sys/types.h>
#include <sys/umtx.h>

#elif defined(HAVE_UMTX_SLEEP)

/* https://man.dragonflybsd.org/?command=umtx&section=2 */

#include <unistd.h>

#elif defined(HAVE___ULOCK_WAIT)

/*
 * This interface is undocumented, but provided by libSystem.dylib since
 * xnu-3789.1.32 (macOS 10.12, 2016) and is used by eg libc++.
 *
 * https://github.com/apple/darwin-xnu/blob/main/bsd/kern/sys_ulock.c
 * https://github.com/apple/darwin-xnu/blob/main/bsd/sys/ulock.h
 */

#include <stdint.h>

#define UL_COMPARE_AND_WAIT_SHARED		3
#define ULF_WAKE_ALL					0x00000100

#ifdef __cplusplus
extern "C"
{
#endif

extern int	__ulock_wait(uint32_t operation,
						 void *addr,
						 uint64_t value,
						 uint32_t timeout);
extern int	__ulock_wake(uint32_t operation,
						 void *addr,
						 uint64_t wake_value);

#ifdef __cplusplus
}
#endif

#endif

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Wait for someone to call pg_futex_wake() for the same address, with an
 * initial check that the value pointed to by 'fut' matches 'value' and an
 * optional timeout.  Returns 0 when woken, and otherwise -1, with errno set to
 * EAGAIN if the initial value check fails, and otherwise errors including
 * EINTR, ETIMEDOUT and EFAULT.
 */
static int
pg_futex_wait_u32(volatile void *fut,
				  uint32 value,
				  struct timespec *timeout)
{
#if defined(HAVE_LINUX_FUTEX_H)
	if (syscall(SYS_futex, fut, FUTEX_WAIT, value, timeout, 0, 0) == 0)
		return 0;
#elif defined(HAVE_SYS_FUTEX_H)
	if ((errno = futex((void *) fut, FUTEX_WAIT, (int) value, timeout, NULL)) == 0)
		return 0;
	if (errno == ECANCELED)
		errno = EINTR;
#elif defined(HAVE_SYS_UMTX_H)
	if (_umtx_op((void *) fut, UMTX_OP_WAIT_UINT, value, 0, timeout) == 0)
		return 0;
#elif defined(HAVE_UMTX_SLEEP)
	if (umtx_sleep((volatile const int *) fut,
				   (int) value,
				   timeout ? timeout->tv_sec * 1000000 + timeout->tv_nsec / 1000 : 0) == 0)
		return 0;
	if (errno == EBUSY)
		errno = EAGAIN;
#elif defined (HAVE___ULOCK_WAIT)
	if (__ulock_wait(UL_COMPARE_AND_WAIT_SHARED,
					 (void *) fut,
					 value,
					 timeout ? timeout->tv_sec * 1000000 + timeout->tv_nsec / 1000 : 0) >= 0)
		return 0;
#else
	/*
	 * If we wanted to simulate futexes on systems that don't have them, here
	 * we could add a link from our PGPROC struct to a shared memory hash
	 * table using "fut" (ie address) as the key, then compare *fut == value.
	 * If false, remove link and fail with EAGAIN.  If true, sleep on proc
	 * latch.  This wouldn't work for DSM segments; for those, we could search
	 * for matching DSM segment mappings in this process, and convert the key
	 * to { segment ID, offset }, just like kernels do internally to make
	 * inter-process futexes work on shared memory, but... ugh.
	 */
	errno = ENOSYS;
#endif

	Assert(errno != 0);

	return -1;
}

/*
 * Wake up to nwaiters waiters that currently wait on the same address as
 * 'fut'.  Returns 0 on success, and -1 on failure, with errno set.  Though
 * some of these interfaces can tell us how many were woken, they can't all do
 * that, so we'll hide that information.
 */
static int
pg_futex_wake(volatile void *fut, int nwaiters)
{
#if defined(HAVE_LINUX_FUTEX_H)
	if (syscall(SYS_futex, fut, FUTEX_WAKE, nwaiters, NULL, 0, 0) >= 0)
		return 0;
#elif defined(HAVE_SYS_FUTEX_H)
	if (futex(fut, FUTEX_WAKE, nwaiters, NULL, NULL) >= 0)
		return 0;
#elif defined(HAVE_SYS_UMTX_H)
	if (_umtx_op((void *) fut, UMTX_OP_WAKE, nwaiters, 0, 0) == 0)
		return 0;
#elif defined(HAVE_UMTX_SLEEP)
	if (umtx_wakeup((volatile const int *) fut, nwaiters) == 0)
		return 0;
#elif defined (HAVE___ULOCK_WAIT)
	if (__ulock_wake(UL_COMPARE_AND_WAIT_SHARED | (nwaiters > 1 ? ULF_WAKE_ALL : 0),
					 (void *) fut,
					 0) >= 0)
		return 0;
	if (errno == ENOENT)
		return 0;
#else
	/* No implementation available. */
	errno = ENOSYS;
#endif

	Assert(errno != 0);

	return -1;
}

#ifdef __cplusplus
}
#endif

#endif							/* PG_FUTEX_H */
