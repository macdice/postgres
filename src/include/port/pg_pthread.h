/*-------------------------------------------------------------------------
 *
 * Declarations for missing POSIX thread components.
 *
 *	  On Windows, this supplies work-alike wrappers of the subset of the POSIX
 *	  threading API that we use.
 *
 *	  On POSIX, this supplies an implementation of pthread_barrier_t for the
 *	  benefit of macOS, which lacks it.  These declarations are not in port.h,
 *	  because that'd require <pthread.h> to be included by every translation
 *	  unit.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PTHREAD_H
#define PG_PTHREAD_H

#ifndef WIN32

#include <pthread.h>

#ifndef HAVE_PTHREAD_BARRIER_WAIT

#ifndef PTHREAD_BARRIER_SERIAL_THREAD
#define PTHREAD_BARRIER_SERIAL_THREAD (-1)
#endif

struct pthread_barrierattr_t;
typedef struct pthread_barrierattr_t pthread_barrierattr_t;

typedef struct pg_pthread_barrier
{
	bool		sense;			/* we only need a one bit phase */
	int			count;			/* number of threads expected */
	int			arrived;		/* number of threads that have arrived */
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} pthread_barrier_t;

extern int	pthread_barrier_init(pthread_barrier_t *barrier,
								 const pthread_barrierattr_t *attr,
								 unsigned count);
extern int	pthread_barrier_wait(pthread_barrier_t *barrier);
extern int	pthread_barrier_destroy(pthread_barrier_t *barrier);

#endif /* HAVE_THREAD_BARRIER_WAIT */

#endif

#ifdef WIN32

#include <handleapi.h>
#include <process.h>

/*
 * For pthread_thread_t, we need to allocate a piece of memory to traffic the
 * argument and result.  We'll use a pointer to that struct as pthread_t.
 *
 * XXX Would it be better to use an index into a dynamically sized array of
 * these?
 */

struct pthread_win32_thunk
{
	HANDLE		handle;
	void	   *(*routine)(void *);
	void	   *arg;
	void	   *result;
};

typedef struct pthread_win32_thunk *pthread_t;

extern __declspec(thread) pthread_t pthread_win32_self;

struct pthread_attr_t;
typedef struct pthread_attr_t pthread_attr_t;

static unsigned __stdcall
pthread_win32_run(void *arg)
{
	pthread_win32_self = (pthread_t) arg;
	pthread_win32_self->result =
		pthread_win32_self->routine(pthread_win32_self->arg);

	return 0;
}

static inline int
pthread_create(pthread_t *thread,
			   const pthread_attr_t *attr,
			   void *(*func)(void *),
			   void *arg)
{
	struct pthread_win32_thunk *th;

	/* Our arg/result transfer object will be freed by pthread_join(). */
	th = malloc(sizeof(*th));
	if (th == NULL)
		return ENOMEM;

	th->routine = func;
	th->arg = arg;
	th->result = NULL;
	th->handle = (HANDLE) _beginthreadex(NULL, 0, pthread_win32_run, th, 0, NULL);
	if (th->handle == 0)
	{
		int			save_errno = errno;

		free(th);
		return save_errno;
	}
	*thread = th;
	return 0;
}

static inline int
pthread_join(pthread_t thread, void **retval)
{
	if (thread == NULL || thread->handle == 0)
		return EINVAL;

	if (WaitForSingleObject(thread, INFINITE) != WAIT_OBJECT_0)
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	if (retval)
		*retval = thread->result;
	CloseHandle(thread->handle);
	free(thread);
	return 0;
}

static inline int
pthread_equal(pthread_t t1, pthread_t t2)
{
	return t1 == t2;
}

static inline pthread_t
pthread_self(void)
{
	return pthread_win32_self;
}

static inline void
pthread_exit(void *value)
{
	pthread_win32_self->result = value;
	_endthreadex(0);
}

/*
 * For pthread_barrier_t we map directly to native synchronization barriers.
 *
 * https://docs.microsoft.com/en-us/windows/win32/sync/synchronization-barriers
 */

struct pthread_barrierattr_t;
typedef struct pthread_barrierattr_t pthread_barrierattr_t;

typedef SYNCHRONIZATION_BARRIER pthread_barrier_t;

static inline int
pthread_barrier_init(pthread_barrier_t *barrier,
					 const pthread_barrierattr_t *attr,
					 unsigned count)
{
	if (InitializeSynchronizationBarrier(barrier, count, 0))
		return 0;
	_dosmaperr(GetLastError());
	return errno;
}

static inline int
pthread_barrier_wait(pthread_barrier_t *barrier)
{
	if (EnterSynchronizationBarrier(barrier,
									SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY))
		return PTHREAD_BARRIER_SERIAL_THREAD;
	return 0;
}

static inline int
pthread_barrier_destroy(pthread_barrier_t *barrier)
{
	/* Nothing to do */
}

/*
 * For pthread_mutex_t, we'll use CRITICAL_SECTION.  Note that these don't
 * work between processes.
 */

typedef struct pthread_mutex_t
{
	LONG state;
	CRITICAL_SECTION critical_section;
} pthread_mutex_t;

#define PTHREAD_MUTEX_UNINITIALIZED		0
#define PTHREAD_MUTEX_INITIALIZING		1
#define PTHREAD_MUTEX_INITIALIZED		2

#define PTHREAD_MUTEX_INITIALIZER {PTHREAD_MUTEX_UNINITIALIZED}

struct pthread_mutexattr_t;
typedef struct pthread_mutexattr_t pthread_mutexattr_t;

static void
pthread_mutex_initialize_on_demand(pthread_mutex_t *mutex)
{
	for (;;)
	{
		/* If initialized already, there's nothing to do. */
		if (likely(mutex->state == PTHREAD_MUTEX_INITIALIZED))
			return;

		/*
		 * Try to become the initializer.  If someone else has beaten us to
		 * it, then we'll spin around the loop hammering the memory bus until
		 * they succeed, which isn't expected to take long.
		 */
		if (InterlockedCompareExchange(&mutex->initialized,
									   PTHREAD_MUTEX_INITIALIZING,
									   PTHREAD_MUTEX_UNINITIALIZED) == 0)
		{
			InitializeCriticalSection(&mutex->critical_section);
			mutex->state = PTHREAD_MUTEX_INITIALIZED;
			return;
		}
	}
	pg_unreachable();
}

static inline int
pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)
{
	InitializeCriticalSection(&mutex->critical_section);
	mutex->state = PTHREAD_MUTEX_INITIALIZED;
	return 0;
}

static inline int
pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	DeleteCriticalSection(&mutex->critical_section);
	return 0;
}

static inline int
pthread_mutex_lock(pthread_mutex_t *mutex)
{
	pthread_mutex_initialize_on_demand(mutex);
	EnterCriticalSection(&mutex->critical_section);
	return 0;
}

static inline int
pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	LeaveCriticalSection(&mutex->critical_section);
	return 0;
}

/*
 * For pthread_once_t, we'll use a double-checked flag and a per-object mutex.
 * We could instead use the equivalent native facility, but there doesn't seem
 * to be any advantage:
 *
 * https://docs.microsoft.com/en-us/windows/win32/sync/using-one-time-initialization
 */

typedef struct pthread_once_t
{
	bool initialized;
	pthread_mutex_t mutex;
} pthread_once_t;

#define PTHREAD_ONCE_INIT {false, PTHREAD_MUTEX_INITIALIZER}

static inline int
pthread_once(pthread_once_t *once_control,
			 void (*init_routine)(void))
{
	if (!once_control->initialized)
	{
		pthread_mutex_lock(&once_control->mutex);
		if (!once_control->initialized)
		{
			init_routine();
			once_control->initialized = true;
		}
		pthread_mutex_unlock(&once_control->mutex);
	}
	return 0;
}

/*
 * For pthread_key_t, we use the newer Fls* functions rather than Tls* because
 * they support destructors, just like POSIX.
 *
 * https://docs.microsoft.com/en-us/windows/win32/api/fibersapi/
 */

typedef DWORD pthread_key_t;

static inline int
pthread_key_create(pthread_key_t *key, void (*destructor)(void *))
{
	if ((*key = FlsAlloc(destructor)) == FLS_OUT_OF_INDEXES)
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
}

static inline int
pthread_key_delete(pthread_key_t key)
{
	if (!FlsFree(key))
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
}

static inline int
pthread_setspecific(pthread_key_t key, const void *value)
{
	if (!FlsSetValue(key, (void *) value))
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
}

static inline void *
pthread_getspecific(pthread_key_t key)
{
	return FlsGetValue(key);
}

#endif

#endif
