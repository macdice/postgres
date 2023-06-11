/*
 * A multi-threading API abstraction loosely based on the C11 standard's
 * <threads.h> header.  The identifiers have a pg_ prefix.  Perhaps one day
 * we'll use standard C threads directly, and we'll drop the prefixes.
 *
 * Exceptions:
 *  - pg_thrd_barrier_t is not based on C11
 */

#ifndef PG_THREADS_H
#define PG_THREADS_H

#ifdef WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#if !defined(WIN32) && !defined(HAVE_PTHREAD_BARRIER_WAIT)

/* macOS lacks pthread_barrier_t, so we define our own. */

#ifndef PTHREAD_BARRIER_SERIAL_THREAD
#define PTHREAD_BARRIER_SERIAL_THREAD (-1)
#endif

typedef struct pg_pthread_barrier
{
	bool		sense;			/* we only need a one bit phase */
	int			count;			/* number of threads expected */
	int			arrived;		/* number of threads that have arrived */
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} pthread_barrier_t;

extern int	pthread_barrier_init(pthread_barrier_t *barrier,
								 const void *attr,
								 int count);
extern int	pthread_barrier_wait(pthread_barrier_t *barrier);
extern int	pthread_barrier_destroy(pthread_barrier_t *barrier);

#endif

#ifdef WIN32
typedef HANDLE pg_thrd_t;
typedef CRITICAL_SECTION pg_mtx_t;
typedef CONDITION_VARIABLE pg_cnd_t;
typedef SYNCHRONIZATION_BARRIER pg_thrd_barrier_t;
typedef DWORD pg_tss_t;
typedef INIT_ONCE pg_once_flag;
#define PG_ONCE_FLAG_INIT INIT_ONCE_STATIC_INIT
#else
typedef pthread_t pg_thrd_t;
typedef pthread_mutex_t pg_mtx_t;
typedef pthread_cond_t pg_cnd_t;
typedef pthread_barrier_t pg_thrd_barrier_t;
typedef pthread_key_t pg_tss_t;
typedef pthread_once_t pg_once_flag;
#define PG_ONCE_FLAG_INIT PTHREAD_ONCE_INIT
#endif

typedef int (*pg_thrd_start_t) (void *);
typedef void (*pg_tss_dtor_t) (void *);
typedef void (*pg_call_once_function_t) (void);

typedef enum pg_thrd_error_t
{
	pg_thrd_success = 0,
	pg_thrd_nomem = 1,
	pg_thrd_timedout = 2,
	pg_thrd_busy = 3,
	pg_thrd_error = 4
} pg_thrd_error_t;

typedef enum pg_mtx_type_t
{
	pg_mtx_plain = 0
} pg_mtx_type_t;

extern int	pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument);
extern int	pg_thrd_join(pg_thrd_t thread, int *result);
extern void pg_thrd_exit(int result);

static inline int
pg_thrd_maperror(int error)
{
#ifdef WIN32
	/* Windows functions generally return TRUE for success. */
	return error ? pg_thrd_success : pg_thrd_error;
#else
	/* POSIX functions generally return 0 for success. */
	return error == 0 ? pg_thrd_success : pg_thrd_error;
#endif
}

#ifdef WIN32
BOOL		pg_call_once_trampoline(pg_once_flag *flag, void *parameter, void **context);
#endif

static inline void
pg_call_once(pg_once_flag *flag, pg_call_once_function_t function)
{
#ifdef WIN32
	InitOnceExecuteOnce(flag, pg_call_once_trampoline, (void *) function, NULL);
#else
	pthread_once(flag, function);
#endif
}

static inline int
pg_thrd_equal(pg_thrd_t lhs, pg_thrd_t rhs)
{
#ifdef WIN32
	return lhs == rhs;
#else
	return pthread_equal(lhs, rhs);
#endif
}

static inline int
pg_tss_create(pg_tss_t *key, pg_tss_dtor_t destructor)
{
#ifdef WIN32
	*key = FlsAlloc(destructor);
	return *key == FLS_OUT_OF_INDEXES ? pg_thrd_error : pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_key_create(key, destructor));
#endif
}

static inline void *
pg_tss_get(pg_tss_t key)
{
#ifdef WIN32
	return FlsGetValue(key);
#else
	return pthread_getspecific(key);
#endif
}

static inline int
pg_tss_set(pg_tss_t key, void *value)
{
#ifdef WIN32
	return pg_thrd_maperror(FlsSetValue(key, value));
#else
	return pg_thrd_maperror(pthread_setspecific(key, value));
#endif
}

static inline int
pg_mtx_init(pg_mtx_t *mutex, int type)
{
#ifdef WIN32
	InitializeCriticalSection(mutex);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_init(mutex, NULL));
#endif
}

static inline int
pg_mtx_lock(pg_mtx_t *mutex)
{
#ifdef WIN32
	EnterCriticalSection(mutex);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_lock(mutex));
#endif
}

static inline int
pg_mtx_unlock(pg_mtx_t *mutex)
{
#ifdef WIN32
	LeaveCriticalSection(mutex);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_unlock(mutex));
#endif
}

static inline int
pg_mtx_destroy(pg_mtx_t *mutex)
{
#ifdef WIN32
	DeleteCriticalSection(mutex);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_destroy(mutex));
#endif
}

static inline int
pg_cnd_init(pg_cnd_t *condvar)
{
#ifdef WIN32
	InitializeConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_init(condvar, NULL));
#endif
}

static inline int
pg_cnd_broadcast(pg_cnd_t *condvar)
{
#ifdef WIN32
	WakeAllConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_broadcast(condvar));
#endif
}

static inline int
pg_cnd_wait(pg_cnd_t *condvar, pg_mtx_t *mutex)
{
#ifdef WIN32
	SleepConditionVariableCS(condvar, mutex, INFINITE);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_wait(condvar, mutex));
#endif
}

static inline int
pg_cnd_destroy(pg_cnd_t *condvar)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_destroy(condvar));
#endif
}

static inline int
pg_thrd_barrier_init(pg_thrd_barrier_t *barrier, int count)
{
#ifdef WIN32
	return pg_thrd_maperror(InitializeSynchronizationBarrier(barrier, count, 0));
#else
	return pg_thrd_maperror(pthread_barrier_init(barrier, NULL, count));
#endif
}

static inline int
pg_thrd_barrier_wait(pg_thrd_barrier_t *barrier)
{
	/*
	 * Windows and POSIX both have a special value returned to only one thread,
	 * but it's not clear how to model that with a C11-style return value,
	 * since pg_thrd_barrier_t is not from C11.  Let's not invent something
	 * unless we eventually need.
	 */
#ifdef WIN32
	EnterSynchronizationBarrier(barrier, SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY);
	return pg_thrd_success;
#else
	int error = pthread_barrier_wait(barrier);
	if (error == 0 || error == PTHREAD_BARRIER_SERIAL_THREAD)
		return pg_thrd_success;
	else
		return pg_thrd_error;
#endif
}

static inline int
pg_thrd_barrier_destroy(pg_thrd_barrier_t *barrier)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_barrier_destroy(barrier));
#endif
}

#endif
