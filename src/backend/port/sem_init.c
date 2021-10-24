/*-------------------------------------------------------------------------
 *
 * sem_init.c
 *
 * Drop-in replacement for POSIX sem_init(), sem_post(), sem_wait() and
 * sem_destroy().  These can be used on systems that don't provide unnamed
 * semaphores with pshared=1, but do have shared memory futexes that we can use
 * to build our own.
 *
 * pg_sem_t is a typedef for pg_futex_u32 in src/include/port/pg_semaphore.h.
 * If we wanted to add a waiter count it could become a struct.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/port/sem_init.c
 *
 *-------------------------------------------------------------------------/
 */

#include "postgres.h"

#ifdef USE_SEMAPHORE_EMULATION

#include "port/atomics.h"
#include "port/pg_futex.h"
#include "port/pg_semaphore.h"

/*
 * Initialize a semaphore with a given value.
 */
int
pg_sem_init(pg_sem_t *semaphore, int pshared, int value)
{
	pg_atomic_init_u32(&semaphore->value, value);
	return 0;
}

/*
 * Destroy a semaphore.
 */
int
pg_sem_destroy(pg_sem_t *semaphore)
{
	return 0;
}

/*
 * Increment the semaphore and wake any waiters.
 *
 * We use semaphores in contexts where there is always a waiter, so we don't
 * bother with a waiter count that could suppress useless system calls here.
 */
int
pg_sem_post(pg_sem_t *semaphore)
{
	pg_atomic_fetch_add_u32(&semaphore->value, 1);
	return pg_futex_wake_u32(semaphore, INT_MAX);
}

/*
 * Decrement the semaphore if it is above zero, or fail with EAGAIN if is
 * already zero.
 */
int
pg_sem_trywait(pg_sem_t *semaphore)
{
	uint32		value = pg_atomic_read_u32(&semaphore->value);

	while (value > 0)
		if (pg_atomic_compare_exchange_u32(&semaphore->value, &value, value - 1))
			return 0;

	errno = EAGAIN;
	return -1;
}

/*
 * Decrement the semaphore, waiting first for it to rise above zero if
 * necessary.
 */
int
pg_sem_wait(pg_sem_t *semaphore)
{
	while (pg_sem_trywait(semaphore) < 0)
		if (pg_futex_wait_u32(semaphore, 0, NULL) < 0)
			return -1;

	return 0;
}

#endif							/* HAVE_EMULATED_SEMAPHORES */
