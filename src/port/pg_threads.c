#include "c.h"
#include "port/pg_threads.h"

#include <errno.h>
#include <stdlib.h>

/*
 * There are small differences between the function types in C11, POSIX (return
 * type) and Windows (return type signedness, calling convention).  The
 * int return value will survive casting to/from void * and DWORD respectively,
 * but we still need a small trampoline function to deal with the different
 * function pointer type.
 */
typedef struct pg_thrd_thunk
{
	pg_thrd_start_t function;
	void	   *argument;
} pg_thrd_thunk;

#ifdef WIN32
BOOL
pg_call_once_trampoline(pg_once_flag *flag, void *parameter, void **context)
{
	pg_call_once_function_t function = (pg_call_once_function_t) parameter;

	function();
	return TRUE;
}
#endif

#ifdef WIN32
static DWORD __stdcall
pg_thrd_trampoline(void *vthunk)
#else
static void *
pg_thrd_trampoline(void *vthunk)
#endif
{
	pg_thrd_thunk *thunk = (pg_thrd_thunk *) vthunk;
	void	   *argument = thunk->argument;
	pg_thrd_start_t function = thunk->function;
	int			result;

	free(vthunk);

	result = function(argument);

#ifdef WIN32
	return (DWORD) result;
#else
	return (void *) (intptr_t) result;
#endif
}

int
pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument)
{
	pg_thrd_thunk *thunk;

	thunk = malloc(sizeof(*thunk));
	if (thunk == NULL)
		return pg_thrd_nomem;
	thunk->function = function;
	thunk->argument = argument;

#ifdef WIN32
	*thread = CreateThread(NULL, 0, pg_thrd_trampoline, thunk, 0, 0);
	if (*thread != NULL)
		return pg_thrd_success;
#else
	if (pthread_create(thread, NULL, pg_thrd_trampoline, thunk) == 0)
		return pg_thrd_success;
#endif

	free(thunk);
	return pg_thrd_error;
}

int
pg_thrd_join(pg_thrd_t thread, int *result)
{
#ifdef WIN32
	DWORD	   dword_result;

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

void
pg_thrd_exit(int result)
{
#ifdef WIN32
	ExitThread((DWORD) result);
#else
	pthread_exit((void *) (intptr_t) result);
#endif
}
