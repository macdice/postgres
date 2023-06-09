/*
 * A multi-threading API abstraction loosely based on the C11 standard's
 * <threads.h> header.  The identifiers have a pg_ prefix.  Perhaps one day
 * we'll use standard C threads directly, and we'll drop the prefixes.
 */

#ifndef PG_THREADS_H
#define PG_THREADS_H

#ifdef WIN32

#else

#include <errno.h>
#include <pthread.h>

typedef pthread_t pg_thrd_t;
typedef pthread_mutex_t pg_mtx_t;
typedef pthread_cond_t pg_cnd_t;
typedef pthread_key_t pg_tss_t;
typedef pthread_once_t pg_once_flag;

typedef int(*pg_thrd_start_t)(void *);
typedef void(*pg_tss_dtor_t)(void *);

#define PG_ONCE_FLAG_INIT PTHREAD_ONCE_INIT

#endif

enum
{
	pg_thrd_success = 0,
	pg_thrd_nomem = 1,
	pg_thrd_timedout = 2,
	pg_thrd_busy = 3,
	pg_thrd_error = 4
};

enum
{
	pg_mtx_plain = 0
};

/*
 * Most of these functions are very thin wrappers, but thread lifetime
 * management requires a little more footwork due to mismatching return types.
 */
extern int pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument);
extern int pg_thrd_join(pg_thrd_t thread, int *result);
extern void pg_thrd_exit(int result);

static inline int
pg_thrd_maperror(int error)
{
	if (error == 0)
		return pg_thrd_success;
	if (error == ENOMEM)
		return pg_thrd_nomem;
	return pg_thrd_error;
}

static inline int
pg_call_once(pg_once_flag *flag, void(*function)(void))
{
	return pg_thrd_maperror(pthread_once(flag, function));
}

static inline int pg_thrd_equal(pg_thrd_t lhs, pg_thrd_t rhs)
{
	return pthread_equal(lhs, rhs);
}

static inline int
pg_tss_create(pg_tss_t *key, pg_tss_dtor_t destructor)
{
	return pg_thrd_maperror(pthread_key_create(key, destructor));
}

static inline void *
pg_tss_get(pg_tss_t key)
{
	return pthread_getspecific(key);
}

static inline int
pg_tss_set(pg_tss_t key, void *value)
{
	return pg_thrd_maperror(pthread_setspecific(key, value));
}

static inline int
pg_mtx_init(pg_mtx_t *mutex, int type)
{
	pthread_mutexattr_t attr;

	pthread_mutexattr_init(&attr);
	return pg_thrd_maperror(pthread_mutex_init(mutex, &attr));
}

static inline int
pg_mtx_lock(pg_mtx_t *mutex)
{
	return pg_thrd_maperror(pthread_mutex_lock(mutex));
}

static inline int
pg_mtx_unlock(pg_mtx_t *mutex)
{
	return pg_thrd_maperror(pthread_mutex_unlock(mutex));
}

static inline int
pg_mtx_destroy(pg_mtx_t *mutex)
{
	return pg_thrd_maperror(pthread_mutex_destroy(mutex));
}

static inline int
pg_cnd_init(pg_cnd_t *condvar)
{
	pthread_condattr_t attr;

	pthread_condattr_init(&attr);
	return pg_thrd_maperror(pthread_cond_init(condvar, &attr));
}

static inline int
pg_cnd_broadcast(pg_cnd_t *condvar)
{
	return pg_thrd_maperror(pthread_cond_broadcast(condvar));
}

static inline int
pg_cnd_wait(pg_cnd_t *condvar, pg_mtx_t *mutex)
{
	return pg_thrd_maperror(pthread_cond_wait(condvar, mutex));
}

static inline int
pg_cnd_destroy(pg_cnd_t *condvar)
{
	return pg_thrd_maperror(pthread_cond_destroy(condvar));
}

#endif
