#include "postgres.h"

#include "port/pg_thread_cond.h"

#ifdef WIN32
#error "not implemented for Win32"
#endif

int
pg_thread_cond_init(pg_thread_cond_t *cond)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_init(cond, pthread_condattr_default) < 0 ? errno : 0;
#else
	return pthread_cond_init(cond, NULL);
#endif
}

int
pg_thread_cond_destroy(pg_thread_cond_t *cond)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_destroy(cond) < 0 ? errno : 0;
#else
	return pthread_cond_destroy(cond);
#endif
}

int
pg_thread_cond_signal(pg_thread_cond_t *cond)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_signal(cond) < 0 ? errno : 0;
#else
	return pthread_cond_signal(cond);
#endif
}

int
pg_thread_cond_broadcast(pg_thread_cond_t *cond)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_broadcast(cond) < 0 ? errno : 0;
#else
	return pthread_cond_broadcast(cond);
#endif
}

int
pg_thread_cond_wait(pg_thread_cond_t *cond,
					pg_thread_mutex_t *mutex)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_wait(cond, mutex) < 0 ? errno : 0;
#else
	return pthread_cond_wait(cond, mutex);
#endif
}

int
pg_thread_cond_timedwait(pg_thread_cond_t *cond,
						 pg_thread_mutex_t *mutex,
						 struct timespec *timeout)
{
#if defined(PTHREAD_DRAFT4)
	return pthread_cond_timedwait(cond, mutex, timeout) < 0 ? errno : 0;
#else
	return pthread_cond_timedwait(cond, mutex, timeout);
#endif
}
