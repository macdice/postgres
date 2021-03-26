#include "postgres.h"

#include "port/pg_thread_cond.h"

#ifdef WIN32
#error "not implemented for Win32"
#endif

int
pg_thread_cond_init(pg_thread_cond_t *cond)
{
	return pthread_cond_init(cond, NULL);
}

int
pg_thread_cond_destroy(pg_thread_cond_t *cond)
{
	return pthread_cond_destroy(cond);
}

int
pg_thread_cond_signal(pg_thread_cond_t *cond)
{
	return pthread_cond_signal(cond);
}

int
pg_thread_cond_broadcast(pg_thread_cond_t *cond)
{
	return pthread_cond_broadcast(cond);
}

int
pg_thread_cond_wait(pg_thread_cond_t *cond,
					pg_thread_mutex_t *mutex)
{
	return pthread_cond_wait(cond, mutex);
}

int
pg_thread_cond_timedwait(pg_thread_cond_t *cond,
						 pg_thread_mutex_t *mutex,
						 struct timespec *timeout)
{
	return pthread_cond_timedwait(cond, mutex, timeout);
}
