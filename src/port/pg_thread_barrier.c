/*-------------------------------------------------------------------------
 *
 * pg_thread_barrier.c
 *    Approximation of pthread_barrier_t using standard C11 primitives.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/port/pg_thread_barrier.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/pg_thread_barrier.h"

int
pg_thread_barrier_init(pg_thread_barrier_t *barrier, int count)
{
	int			error;

	barrier->sense = false;
	barrier->count = count;
	barrier->arrived = 0;
	if ((error = pg_cnd_init(&barrier->cond)) != pg_thrd_success)
		return error;
	if ((error = pg_mtx_init(&barrier->mutex, pg_mtx_plain)) != pg_thrd_success)
	{
		pg_cnd_destroy(&barrier->cond);
		return error;
	}

	return 0;
}

int
pg_thread_barrier_wait(pg_thread_barrier_t *barrier)
{
	bool		initial_sense;

	pg_mtx_lock(&barrier->mutex);

	/* We have arrived at the barrier. */
	barrier->arrived++;
	Assert(barrier->arrived <= barrier->count);

	/* If we were the last to arrive, release the others and return. */
	if (barrier->arrived == barrier->count)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pg_mtx_unlock(&barrier->mutex);
		pg_cnd_broadcast(&barrier->cond);

		return PG_THREAD_BARRIER_SERIAL_THREAD;
	}

	/* Wait for someone else to flip the sense. */
	initial_sense = barrier->sense;
	do
	{
		pg_cnd_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);

	pg_mtx_unlock(&barrier->mutex);

	return 0;
}

int
pg_thread_barrier_destroy(pg_thread_barrier_t *barrier)
{
	pg_cnd_destroy(&barrier->cond);
	pg_mtx_destroy(&barrier->mutex);
	return 0;
}
