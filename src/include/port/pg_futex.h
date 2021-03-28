/*
 * A very thin portable abstraction over futex APIs.
 */

#ifndef PG_FUTEX_H
#define PG_FUTEX_H

#include "port/atomics.h"

#include <signal.h>

#if defined(HAVE_LINUX_FUTEX_H)

#define HAVE_PG_FUTEX_T

/* Linux's futexes use uint32_t, even on 64 bit systems. */
typedef uint32 pg_futex_t;
#define SIZEOF_PG_FUTEX_T 4

typedef pg_atomic_uint32 pg_atomic_futex_t;
#define pg_atomic_init_futex pg_atomic_init_u32
#define pg_atomic_read_futex pg_atomic_read_u32
#define pg_atomic_fetch_add_futex pg_atomic_fetch_add_u32

#endif

#if defined(HAVE_PG_FUTEX_T)
extern int pg_futex_wait(volatile pg_atomic_futex_t *fut,
						 pg_futex_t value,
						 struct timespec *timeout);

extern int pg_futex_wake(volatile pg_atomic_futex_t *fut, int nwaiters);

extern volatile sig_atomic_t please_increment_futex;
extern volatile pg_atomic_futex_t *futex_to_increment;

/*
 * Before reading the current value of a futex and going to sleep, a process
 * should call this function to make sure that signal handlers that call
 * pg_signal_handler_futex_interrupt() will increment the futex and prevent us
 * from sleeping, closing a race.
 */
static inline void
pg_prepare_futex(volatile pg_atomic_futex_t *fut)
{
	futex_to_increment = fut;
	please_increment_futex = true;
}

/*
 * If no longer sleeping, we can disarm the signal handler communication.
 */
static inline void
pg_finish_futex(void)
{
	please_increment_futex = false;
	futex_to_increment = NULL;
}

/*
 * Signal handlers that want to make sure they wake futex waiters should
 * call this.  This doesn't do anything useful if we're already sleeping (in
 * that case pg_futex_wait() should return with errno EINTR), but it's useful
 * if the signal handle runs in between pg_prepare_futex() and pg_futex_wait(),
 * to prevent the latter from sleeping.
 */
static inline void
pg_signal_handler_futex_interrupt(void)
{
	if (please_increment_futex)
		pg_atomic_fetch_add_futex(futex_to_increment, 1);
}

#endif

#endif
