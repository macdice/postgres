#include "postgres.h"

#include "port.h"
#include "port/pg_thread.h"

#if defined(WIN32)
#include <windows.h>
#endif

#if defined(WIN32)
struct pg_thread_win32_thunk
{
	HANDLE		handle;
	void	   *(*routine)(void *);
	void	   *arg;
	void	   *result;
};

static __declspec(thread) struct pg_thread_win32_thunk *pg_thread_win32_self;

static unsigned __stdcall
pg_thread_win32_run(void *arg)
{
	pg_thread_win32_self->result =
		pg_thread_win32_self->routine(pg_thread_win32_self->arg);

	return 0;
}
#endif

int
pg_thread_create(pg_thread_t *thread, void *(*func)(void *), void *arg)
{
#if defined(WIN32)
	struct pg_thread_win32_thunk *th;

	th = malloc(sizeof(*th));
	if (th == NULL)
		return ENOMEM;

	th->routine = func;
	th->arg = arg;
	th->result = NULL;
	th->handle = (HANDLE) _beginthreadex(NULL, 0, pg_thread_win32_run, th, 0, NULL);
	if (th->handle == 0)
	{
		int			save_errno = errno;

		free(th);
		return save_errno;
	}
	*thread = th;
	return 0;
#elif defined(PTHREAD_DRAFT4)
	return pthread_create(thread, pthread_attr_default, func, arg) < 0 ? errno : 0;
#else
	return pthread_create(thread, NULL, func, arg);
#endif
}

int
pg_thread_join(pg_thread_t thread, void **retval)
{
#if defined(WIN32)
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
#elif defined(PTHREAD_DRAFT4)
	return pthread_join(thread, retval) < 0 ? errno : 0;
#else
	return pthread_join(thread, retval);
#endif
}

int
pg_thread_equal(pg_thread_t t1, pg_thread_t t2)
{
#if defined(WIN32)
	return t1 == t2;
#else
	return pthread_equal(t1, t2);
#endif
}

pg_thread_t
pg_thread_self(void)
{
#if defined(WIN32)
	return pg_thread_win32_self;
#else
	return pthread_self();
#endif
}

void
pg_thread_exit(void *value)
{
#if defined(WIN32)
	pg_thread_win32_self->result = value;
	_endthreadex(0);
#elif defined(PTHREAD_DRAFT4)
	pthread_exit((pthread_addr_t) value);
#else
	pthread_exit(value);
#endif
}
