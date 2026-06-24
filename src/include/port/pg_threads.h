/*-------------------------------------------------------------------------
 *
 * pg_threads.h
 *    Portable multi-threading API.
 *
 * A multi-threading API abstraction based on a subset of C11 <threads.h>,
 * with some extensions.  The identifiers have a pg_ prefix but otherwise
 * follow C11 naming.
 *
 * We have some extensions of our own, not present in C11:
 *
 * - pg_rwlock_t for read/write locks
 * - pg_mtx_t has initialization value PG_MTX_STATIC_INIT
 * - pg_barrier_t
 *
 * For thread_local, see c.h.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/port/pg_threads.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_THREADS_H
#define PG_THREADS_H

#if defined(WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif


/*-------------------------------------------------------------------------
 *
 * Return values.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 thrd_error_t. */
typedef enum pg_thrd_error_t
{
	pg_thrd_success = 0,
	pg_thrd_nomem = 1,
	pg_thrd_timedout = 2,
	pg_thrd_busy = 3,
	pg_thrd_error = 4,
} pg_thrd_error_t;

/* Convert native error to C11 error. */
static inline int
pg_thrd_maperror(int error)
{
#ifdef WIN32
	return error ? pg_thrd_success : pg_thrd_error;
#else
	return error == 0 ? pg_thrd_success : pg_thrd_error;
#endif
}


/*-------------------------------------------------------------------------
 *
 * Threads.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 thrd_t.  Uses native thread identifier. */
#ifdef WIN32
typedef HANDLE pg_thrd_t;
#else
typedef pthread_t pg_thrd_t;
#endif

/* Like C11 thrd_start_t. */
typedef int (*pg_thrd_start_t) (void *);

/* Like C11 thrd_create(), thrd_join(). */
extern int	pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument);
extern int	pg_thrd_join(pg_thrd_t thread, int *result);

#ifdef WIN32
extern pg_thrd_t pg_thrd_current_win32(void);
#endif

/* Like C11 thrd_current(). */
static inline pg_thrd_t
pg_thrd_current(void)
{
#ifdef WIN32
	return pg_thrd_current_win32();
#else
	return pthread_self();
#endif
}

/* Like C11 thrd_equal(). */
static inline int
pg_thrd_equal(pg_thrd_t lhs, pg_thrd_t rhs)
{
#ifdef WIN32
	return lhs == rhs;
#else
	return pthread_equal(lhs, rhs);
#endif
}

/* Like C11 thrd_exit(). */
static inline void
pg_thrd_exit(int result)
{
#ifdef WIN32
	ExitThread((DWORD) result);
#else
	pthread_exit((void *) (intptr_t) result);
#endif
}


/*-------------------------------------------------------------------------
 *
 * Initialization functions.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 once_flag, ONCE_FLAG_INIT. */
#ifdef WIN32
typedef INIT_ONCE pg_once_flag;
#define PG_ONCE_FLAG_INIT INIT_ONCE_STATIC_INIT
#else
typedef pthread_once_t pg_once_flag;
#define PG_ONCE_FLAG_INIT PTHREAD_ONCE_INIT
#endif

/* Like C11 once_function_t. */
typedef void (*pg_call_once_function_t) (void);

#ifdef WIN32
/* Windows helper that deals with function type mismatch. */
extern BOOL CALLBACK pg_call_once_trampoline(pg_once_flag *flag,
											 void *parameter,
											 void **context);
#endif

/* Like C11 call_once(). */
static inline void
pg_call_once(pg_once_flag *flag, pg_call_once_function_t function)
{
#ifdef WIN32
	InitOnceExecuteOnce(flag, pg_call_once_trampoline, (void *) function, NULL);
#else
	pthread_once(flag, function);
#endif
}


/*-------------------------------------------------------------------------
 *
 * Thread-specific storage.  This mechanism is an alternative to using
 * the thread_local storage class, which should be preferred where possible.
 * The only advantage is that the TSS interface allows a destructor function
 * to be run for non-NULL values when the thread exits.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 tss_t. */
#ifdef WIN32
typedef DWORD pg_tss_t;
#else
typedef pthread_key_t pg_tss_t;
#endif

/* Like C11 tss_dtor_t. */
typedef void (*pg_tss_dtor_t) (void *);

#ifdef WIN32
/* Windows helpers that deal with destructor API differences. */
extern int	pg_tss_win32_create(pg_tss_t *tss_id, pg_tss_dtor_t destructor);
extern void pg_tss_win32_delete(pg_tss_t tss_id);
#endif

/* Like C11 TSS_DTOR_ITERATIONS. */
#ifdef WIN32
#define PG_TSS_DTOR_ITERATIONS 1
#else
#define PG_TSS_DTOR_ITERATIONS PTHREAD_DESTRUCTOR_ITERATIONS
#endif

/* Like C11 tss_create(). */
static inline int
pg_tss_create(pg_tss_t *tss_id, pg_tss_dtor_t destructor)
{
#ifdef WIN32
	return pg_tss_win32_create(tss_id, destructor);
#else
	return pg_thrd_maperror(pthread_key_create(tss_id, destructor));
#endif
}

/* Like C11 tss_delete(). */
static inline void
pg_tss_delete(pg_tss_t tss_id)
{
#ifdef WIN32
	pg_tss_win32_delete(tss_id);
#else
	pthread_key_delete(tss_id);
#endif
}

/* Like C11 tss_get(). */
static inline void *
pg_tss_get(pg_tss_t tss_id)
{
#ifdef WIN32
	return FlsGetValue(tss_id);
#else
	return pthread_getspecific(tss_id);
#endif
}

/* Like C11 tss_set(). */
static inline int
pg_tss_set(pg_tss_t tss_id, void *value)
{
#ifdef WIN32
	return pg_thrd_maperror(FlsSetValue(tss_id, value));
#else
	return pg_thrd_maperror(pthread_setspecific(tss_id, value));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Read/write locks.  Not in C11.
 *
 * Unfortunately Windows makes you say whether you're unlocking a read lock or
 * a write lock, so we have to expose that here too.  POSIX already knows.
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
typedef SRWLOCK pg_rwlock_t;
#define PG_RWLOCK_STATIC_INIT SRWLOCK_INIT
#else
typedef pthread_rwlock_t pg_rwlock_t;
#define PG_RWLOCK_STATIC_INIT PTHREAD_RWLOCK_INITIALIZER
#endif

static inline int
pg_rwlock_init(pg_rwlock_t *lock)
{
#ifdef WIN32
	InitializeSRWLock(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_init(lock, NULL));
#endif
}

static inline int
pg_rwlock_rlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	AcquireSRWLockShared(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_rdlock(lock));
#endif
}

static inline int
pg_rwlock_wlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	AcquireSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_wrlock(lock));
#endif
}

static inline int
pg_rwlock_wunlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	ReleaseSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_unlock(lock));
#endif
}

static inline int
pg_rwlock_runlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	ReleaseSRWLockShared(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_unlock(lock));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Simple mutexes.
 *
 *-------------------------------------------------------------------------
 */

/*
 * C11 doesn't define a static initializer, but it is very convenient to have
 * one.
 */
#ifdef WIN32
/*
 * CRITICAL_SECTION might be the most obvious Windows mechanism for pg_mtx_t,
 * but SRWLock is reported to be at least as fast when used only in exclusive
 * mode, and has the advantage of a static initializer (CRITICAL_SECTION must
 * be initialized and destroyed explicitly because it allocates kernel
 * resource).  So we'll just point pg_mtx_t to pg_rwlock_t on Windows.
 */
typedef pg_rwlock_t pg_mtx_t;
#define PG_MTX_STATIC_INIT PG_RWLOCK_STATIC_INIT
#else
typedef pthread_mutex_t pg_mtx_t;
#define PG_MTX_STATIC_INIT PTHREAD_MUTEX_INITIALIZER
#endif

/* Like C11 mtx_type_t. */
typedef enum pg_mtx_type_t
{
	pg_mtx_plain = 0
} pg_mtx_type_t;

/* Like C11 mtx_init(). */
static inline int
pg_mtx_init(pg_mtx_t *mutex, int type)
{
#ifdef WIN32
	return pg_rwlock_init(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_init(mutex, NULL));
#endif
}

/* Like C11 mtx_lock(). */
static inline int
pg_mtx_lock(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_rwlock_wlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_lock(mutex));
#endif
}

/* Like C11 mtx_unlock(). */
static inline int
pg_mtx_unlock(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_rwlock_wunlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_unlock(mutex));
#endif
}

/* Like C11 mtx_destroy(). */
static inline int
pg_mtx_destroy(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_destroy(mutex));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Condition variables.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 cnd_t. */
#ifdef WIN32
typedef CONDITION_VARIABLE pg_cnd_t;
#else
typedef pthread_cond_t pg_cnd_t;
#endif

/* Like C11 cnd_init(). */
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

/* Like C11 cnd_broadcast(). */
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

/* Like C11 cnd_wait(). */
static inline int
pg_cnd_wait(pg_cnd_t *condvar, pg_mtx_t *mutex)
{
#ifdef WIN32
	SleepConditionVariableSRW(condvar, mutex, INFINITE, 0);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_wait(condvar, mutex));
#endif
}

/* Like C11 cnd_destroy(). */
static inline int
pg_cnd_destroy(pg_cnd_t *condvar)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_destroy(condvar));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Barriers.  Not in C11.  This is part of the POSIX "advanced realtime
 * threads" API that is missing on macOS, so a fallback implementation is
 * provided.
 *
 *-------------------------------------------------------------------------
 */

/* A thread synchronization barrier. */
#ifdef WIN32
typedef SYNCHRONIZATION_BARRIER pg_barrier_t;
#elif defined(HAVE_PTHREAD_BARRIER_WAIT)
typedef pthread_barrier_t pg_barrier_t;
#else
typedef struct pg_barrier_t
{
	bool		sense;
	int			expected;
	int			arrived;
	pg_mtx_t	mutex;
	pg_cnd_t	cond;
} pg_barrier_t;
#endif

/*
 * Initialize a thread synchronization barrier that waits for 'count' threads
 * when pg_barrier_wait() is called.
 */
static inline int
pg_barrier_init(pg_barrier_t *barrier, int count)
{
#ifdef WIN32
	return pg_thrd_maperror(InitializeSynchronizationBarrier(barrier, count, 0));
#elif defined(HAVE_PTHREAD_BARRIER_WAIT)
	return pg_thrd_maperror(pthread_barrier_init(barrier, NULL, count));
#else
	barrier->sense = false;
	barrier->expected = count;
	barrier->arrived = 0;
	if (pg_cnd_init(&barrier->cond) != pg_thrd_success)
		return pg_thrd_error;
	if (pg_mtx_init(&barrier->mutex, pg_mtx_plain) != pg_thrd_success)
	{
		pg_cnd_destroy(&barrier->cond);
		return pg_thrd_error;
	}
	return pg_thrd_success;
#endif
}

/*
 * Wait for all expected threads to arrive at the barrier, and elect one
 * arbitrary thread to perform a phase of computation serially.  Sets
 * *elected_thread to true in the elected thread, and false in all others.
 */
static inline int
pg_barrier_wait_and_elect(pg_barrier_t *barrier, bool *elected_thread)
{
#ifdef WIN32
	if (EnterSynchronizationBarrier(barrier,
									SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY))
		*elected_thread = true;
	else
		*elected_thread = false;
	return pg_thrd_success;
#elif defined(HAVE_PTHREAD_BARRIER_WAIT)
	int			error = pthread_barrier_wait(barrier);

	if (error == 0)
	{
		*elected_thread = false;
		return pg_thrd_success;
	}
	else if (error == PTHREAD_BARRIER_SERIAL_THREAD)
	{
		*elected_thread = true;
		return pg_thrd_success;
	}
	else
	{
		return pg_thrd_error;
	}
#else
	bool		initial_sense;

	pg_mtx_lock(&barrier->mutex);
	barrier->arrived++;
	if (barrier->arrived == barrier->expected)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pg_mtx_unlock(&barrier->mutex);
		pg_cnd_broadcast(&barrier->cond);
		*elected_thread = true;
		return pg_thrd_success;
	}
	initial_sense = barrier->sense;
	do
	{
		pg_cnd_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);
	pg_mtx_unlock(&barrier->mutex);
	*elected_thread = false;
	return pg_thrd_success;
#endif
}

/* Wait for all threads to arrive at the barrier. */
static inline int
pg_barrier_wait(pg_barrier_t *barrier)
{
	bool		elected_thread pg_attribute_unused();

	return pg_barrier_wait_and_elect(barrier, &elected_thread);
}

/* Destroy a barrier. */
static inline int
pg_barrier_destroy(pg_barrier_t *barrier)
{
#ifdef WIN32
	return pg_thrd_success;
#elif defined(HAVE_PTHREAD_BARRIER_WAIT)
	return pg_thrd_maperror(pthread_barrier_destroy(barrier));
#else
	pg_mtx_destroy(&barrier->mutex);
	pg_cnd_destroy(&barrier->cond);
	return pg_thrd_success;
#endif
}

#endif
