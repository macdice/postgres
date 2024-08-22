/*-------------------------------------------------------------------------
 *
 * pg_threads.c
 *    Out-of-line parts of portable multi-threading API.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/port/pg_threads.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "port/pg_threads.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>


/*-------------------------------------------------------------------------
 *
 * Threads.
 *
 * There are small differences between the function types in C11,
 * POSIX (return type) and Windows (return type signedness, calling
 * convention).  The int return value will survive casting to/from
 * void * and DWORD respectively, but we still need a small trampoline
 * function to deal with the different function pointer type.
 *
 *-------------------------------------------------------------------------
 */

typedef struct pg_thrd_start_info
{
	pg_thrd_start_t function;
	void	   *argument;

#ifdef PG_THREADS_WIN32
	/*
	 * A place for the thread's own handle to be passed from parent thread to
	 * child thread.  We assume that this can be stored and loaded atomically,
	 * which is true on the relevant architectures, because HANDLEs are
	 * pointer-sized.
	 */
	pg_thrd_t self;

	/*
	 * In the unlikely event that pg_thrd_current() is called in the child
	 * thread before the parent thread has set it, we need a wait to wait for
	 * it.
	 */
	pg_mtx_t mutex;
	pg_cnd_t cond;
#endif
} pg_thrd_start_info;

#ifdef PG_THREADS_WIN32
static pg_thread_local pg_thrd_t my_thrd_handle;
#endif

/*
 * A trampoline function, to handle calling convention and parameter
 * variations in the native APIs.
 */
#ifdef PG_THREADS_WIN32
static DWORD __stdcall
pg_thrd_body(void *thunk)
#else
static void *
pg_thrd_body(void *thunk)
#endif
{
	pg_thrd_start_info *start_info = (pg_thrd_start_info *) thunk;
	pg_thrd_start_t function = start_info->function;
	void	   *argument = start_info->argument;
	int			result;

#ifdef PG_THREADS_WIN32
	/*
	 * Windows threads don't know their own handle, and can't get it directly.
	 * So wait for pg_thrd_create() to give it to us.  Hopefully it is was
	 * stored before we even started running and we won't need to sleep.
	 */
	if (start_info->self == NULL)
	{
		pg_mtx_lock(&my_thrd_start_info->mutex);
		while (my_thrd_start_info->self == NULL)
			pg_cnd_wait(&my_thrd_start_info->mutex,
						&my_thrd_start_info->cond);
		pg_mtx_unlock(&my_thrd_start_info->mutex);
	}
	my_thrd_handle = start_info->self;
#endif

	free(start_info);

	result = function(argument);

#ifdef PG_THREADS_WIN32
	return (DWORD) result;
#else
	return (void *) (intptr_t) result;
#endif
}

int
pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument)
{
	pg_thrd_start_info *start_info;

#ifdef PG_THREADS_WIN32
	/*
	 * Make sure that the thread-exit callback will run, if we haven't set it
	 * up already.
	 */
	if (!pg_thrd_exit_callback_installed)
	{
		pg_mtx_lock(pg_thrd_start_lock);
		if (!pg_thrd_exit_callback_installed)
		{
			pg_tss_t throwaway_tss;
			if (pg_tss_create(&tss, pg_thrd_exit_callback) == pg_thrd_success)
				pg_thrd_exit_callback_installed;
		}
		pg_mtx_unlock(pg_thrd_start_lock);
		if (!pg_thrd_exit_callback_installed)
			return pg_thrd_nomem;
	}
#endif

	start_info = malloc(sizeof(*start_info));
	if (start_info == NULL)
		return pg_thrd_nomem;
	start_info->function = function;
	start_info->argument = argument;

#ifdef PG_THREADS_WIN32
	start_info->self = NULL;
	pg_mtx_init(&start_info->mutex);
	pg_cnd_init(&start_info->cond);

	*thread = CreateThread(NULL, 0, pg_thrd_body, start_info, 0, 0);
	if (*thread != NULL)
	{
		/* Tell the thread what its own handle is. */
		pg_mtx_lock(&start_info->mutex);
		start_info->self = *thread;
		pg_mtx_unlock(&start_info->mutex);
		pg_cnd_broadcast(&start_info->cond);

		return pg_thrd_success;
	}
#else
	if (pthread_create(thread, NULL, pg_thrd_body, start_info) == 0)
		return pg_thrd_success;
#endif

	free(start_info);
	return pg_thrd_error;
}

int
pg_thrd_join(pg_thrd_t thread, int *result)
{
#ifdef WIN32
	DWORD		dword_result;

	if (WaitForSingleObject(thread, INFINITE) == WAIT_OBJECT_0)
	{
		if (result)
		{
			if (!GetExitCodeThread(thread, &dword_result))
				return pg_thrd_error;
			*result = (int) dword_result;
		}
		CloseHandle(thread);
		return pg_thrd_success;
	}
#else
	void	   *void_star_result;

	if (pthread_join(thread, &void_star_result) == 0)
	{
		if (result)
			*result = (int) (intptr_t) void_star_result;
		return pg_thrd_success;
	}
#endif
	return pg_thrd_error;
}

#ifdef PG_THREADS_WIN32
pg_thrd_t
pg_thrd_current_win32(void)
{
	/*
	 * In .c file to avoid potential DLL complications if pg_thread_local
	 * access is inlined.
	 */
	return my_thrd_handle;
}
#endif


/*-------------------------------------------------------------------------
 *
 * Initialization functions.
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
BOOL		CALLBACK
pg_call_once_trampoline(pg_once_flag *flag, void *parameter, void **context)
{
	pg_call_once_function_t function = (pg_call_once_function_t) parameter;

	function();
	return TRUE;
}
#endif

