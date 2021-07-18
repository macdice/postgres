/*
 * A very thin portable abstraction over futex APIs.
 */

#ifndef PG_FUTEX_H
#define PG_FUTEX_H

#include "port/atomics.h"

#include <signal.h>
#include <time.h>

#if defined(HAVE_LINUX_FUTEX_H) || \
	defined(HAVE_SYS_FUTEX_H) || \
	defined(HAVE_SYS_UMTX_H) || \
	defined(HAVE___ULOCK_WAIT)

#define HAVE_PG_FUTEX_T

#if defined(HAVE_LINUX_FUTEX_H) || defined(HAVE_SYS_FUTEX_H)
/* Linux and OpenBSD's futexes use uint32_t, even on 64 bit systems. */
typedef uint32 pg_futex_t;
#define SIZEOF_PG_FUTEX_T 4
#elif defined(HAVE_SYS_UMTX_H)
/* FreeBSD's user space mutexes use long. */
typedef long pg_futex_t;
#define SIZEOF_PG_FUTEX_T SIZEOF_LONG
#elif defined(HAVE___ULOCK_WAIT)
/* macOS's user locks use uint64_t (or optionally uint32_t). */
typedef uint64 pg_futex_t;
#define SIZEOF_PG_FUTEX_T 8
#endif

#if SIZEOF_PG_FUTEX_T == 4
typedef pg_atomic_uint32 pg_atomic_futex_t;
#define pg_atomic_init_futex pg_atomic_init_u32
#define pg_atomic_read_futex pg_atomic_read_u32
#define pg_atomic_write_futex pg_atomic_write_u32
#define pg_atomic_fetch_add_futex pg_atomic_fetch_add_u32
#else
typedef pg_atomic_uint64 pg_atomic_futex_t;
#define pg_atomic_init_futex pg_atomic_init_u64
#define pg_atomic_read_futex pg_atomic_read_u64
#define pg_atomic_write_futex pg_atomic_write_u64
#define pg_atomic_fetch_add_futex pg_atomic_fetch_add_u64
#endif

#endif

#if defined(HAVE_PG_FUTEX_T)
extern int pg_futex_wait(volatile pg_atomic_futex_t *fut,
						 pg_futex_t value,
						 struct timespec *timeout);

extern int pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters);

/*
 * In order to make sure that we don't sleep if a signal arrives in between
 * checking a futex value and entering the kernel with pg_futex_wait(), we
 * allow signal handlers to adjust the futex value so that pg_futex_wait()
 * returns immediately.
 */
#define PG_FUTEX_INTERRUPT_OP_NONE 0
#define PG_FUTEX_INTERRUPT_OP_SET 1
#define PG_FUTEX_INTERRUPT_OP_INC 2
#define PG_FUTEX_INTERRUPT_OP_CLEAR 3
extern volatile sig_atomic_t pg_futex_interrupt_op;
extern volatile pg_atomic_futex_t *pg_futex_interruptible;

/*
 * Before reading the current value of a futex and going to sleep, a process
 * should call this function to make sure that signal handlers that call
 * pg_signal_handler_futex_interrupt() will increment the futex and prevent us
 * from sleeping, closing a race.
 */
static inline void
pg_futex_set_interruptible(volatile pg_atomic_futex_t *fut, int op)
{
	pg_futex_interruptible = fut;
	pg_futex_interrupt_op = op;
}

/*
 * If no longer sleeping, we can disarm the signal handler communication.
 */
static inline void
pg_futex_clear_interruptible(void)
{
	pg_futex_interrupt_op = PG_FUTEX_INTERRUPT_OP_NONE;
	pg_futex_interruptible = NULL;
}

/*
 * Signal handlers that want to make sure they wake futex waiters should
 * call this.  This doesn't do anything useful if we're already sleeping (in
 * that case pg_futex_wait() should return with errno EINTR), but it's useful
 * if the signal handle runs in between pg_futex_set_interruptible() and
 * pg_futex_wait(), to prevent the latter from sleeping.
 */
static inline void
pg_signal_handler_futex_interrupt(void)
{
	volatile pg_atomic_futex_t *fut = pg_futex_interruptible;

	switch (pg_futex_interrupt_op)
	{
	case PG_FUTEX_INTERRUPT_OP_SET:
		pg_atomic_write_futex(fut, 1);
		break;
	case PG_FUTEX_INTERRUPT_OP_INC:
		pg_atomic_fetch_add_futex(fut, 1);
		break;
	case PG_FUTEX_INTERRUPT_OP_CLEAR:
		pg_atomic_write_futex(fut, 0);
		break;
	}
}

#endif

#endif
