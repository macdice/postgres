#include "postgres.h"

#include "port/pg_thread_mutex.h"

#ifdef WIN32
static void
pg_thread_mutex_initialize_on_demand(pg_thread_mutex_t *mutex)
{
	for (;;)
	{
		/* If initialized, we're done. */
		if (likely(mutex->initialized == 2))
			return;


		/* If someone else is initializing it, spin. */
		while (mutex->initialized == 1)
			;

		/* Try to become the initializer. */
		if (InterlockedCompareExchange(&mutex->initialized, 1, 0) == 0)
		{
			InitializeCriticalSection(&mutex->critical_section);
			mutex->initialized = 2;
			return;
		}
	}

	pg_unreachable();
}
#endif

int
pg_thread_mutex_init(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	InitializeCriticalSection(&mutex->critical_section);
	mutex->initialized = 2;
	return 0;
#else
	return pthread_mutex_init(mutex, NULL);
#endif
}

int
pg_thread_mutex_destroy(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	DeleteCriticalSection(&mutex->critical_section);
	return 0;
#else
	return pthread_mutex_destroy(mutex);
#endif
}

int
pg_thread_mutex_lock(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	/*
	 * We may need to initialize on the fly, to handle
	 * PG_THREAD_MUTEX_INITIALIZER.
	 */
	pg_thread_mutex_initialize_on_demand(mutex);
	EnterCriticalSection(&mutex->critical_section);
	return 0;
#else
	return pthread_mutex_lock(mutex);
#endif
}

int
pg_thread_mutex_unlock(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	LeaveCriticalSection(&mutex->critical_section);
	return 0;
#else
	return pthread_mutex_unlock(mutex);
#endif
}
