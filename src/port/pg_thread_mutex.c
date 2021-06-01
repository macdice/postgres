#include "postgres.h"

#include "port/pg_thread_mutex.h"

int
pg_thread_mutex_init(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	InitializeCriticalSection(mutex);
	return 0;
#elif defined(PTHREAD_DRAFT4)
	return pthread_mutex_init(mutex, pthread_mutexattr_default) < 0 ? errno : 0;
#else
	return pthread_mutex_init(mutex, NULL);
#endif
}

int
pg_thread_mutex_destroy(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	DeleteCriticalSection(mutex);
	return 0;
#elif defined(PTHREAD_DRAFT4)
	return pthread_mutex_destroy(mutex) < 0 ? errno : 0;
#else
	return pthread_mutex_destroy(mutex);
#endif
}

int
pg_thread_mutex_lock(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	EnterCriticalSection(mutex);
	return 0;
#elif defined(PTHREAD_DRAFT4)
	return pthread_mutex_lock(mutex) < 0 ? errno : 0;
#else
	return pthread_mutex_lock(mutex);
#endif
}

int
pg_thread_mutex_unlock(pg_thread_mutex_t *mutex)
{
#if defined(WIN32)
	LeaveCriticalSection(mutex);
	return 0;
#elif defined(PTHREAD_DRAFT4)
	return pthread_mutex_unlock(mutex) < 0 ? errno : 0;
#else
	return pthread_mutex_unlock(mutex);
#endif
}
