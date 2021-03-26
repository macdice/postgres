#include "postgres.h"

#include "port/pg_thread_barrier.h"

int
pg_thread_barrier_init(pg_thread_barrier_t *barrier, int count)
{
#if defined(WIN32)
	if (InitializeSynchronizationBarrier(barrier, count, 0))
		return 0;
	_dosmaperr(GetLastError());
	return errno;
#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)
	int			error;
	barrier->sense = false;
	barrier->count = count;
	barrier->arrived = 0;
	if ((error = pg_thread_cond_init(&barrier->cond)) != 0)
		return error;
	if ((error = pg_thread_mutex_init(&barrier->mutex)) != 0)
	{
		pg_thread_cond_destroy(&barrier->cond);
		return error;
	}
	return 0;
#else
	return pthread_barrier_init(barrier, NULL, count);
#endif
}

int
pg_thread_barrier_wait(pg_thread_barrier_t *barrier)
{
#if defined(WIN32)
	if (EnterSynchronizationBarrier(barrier,
									SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY))
		return PG_THREAD_BARRIER_SERIAL_THREAD;
	return 0;
#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)
	bool		initial_sense;

	pg_thread_mutex_lock(&barrier->mutex);

	/* We have arrived at the barrier. */
	barrier->arrived++;
	Assert(barrier->arrived <= barrier->count);

	/* If we were the last to arrive, release the others and return. */
	if (barrier->arrived == barrier->count)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pg_thread_mutex_unlock(&barrier->mutex);
		pg_thread_cond_broadcast(&barrier->cond);

		return PTHREAD_BARRIER_SERIAL_THREAD;
	}

	/* Wait for someone else to flip the sense. */
	initial_sense = barrier->sense;
	do
	{
		pthread_cond_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);

	pg_thread_mutex_unlock(&barrier->mutex);

	return 0;
#else
	return pthread_barrier_wait(barrier);
#endif
}

int
pg_thread_barrier_destroy(pg_thread_barrier_t *barrier)
{
#if defined(WIN32)
	/* not freed, XXX */
	return 0;
#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)
	pg_thread_cond_destroy(&barrier->cond);
	pg_thread_mutex_destroy(&barrier->mutex);
	return 0;
#else
	return pthread_barrier_destroy(barrier);
#endif
}
