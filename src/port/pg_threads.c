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

#ifdef WIN32
	/* Space to pass the thread's handle, for use by pg_thrd_current(). */
	pg_thrd_t	self;
#endif
}			pg_thrd_start_info;

#ifdef WIN32
static thread_local pg_thrd_t my_thrd_handle;
#endif

/*
 * A trampoline function, to handle calling convention and parameter
 * variations in the native APIs.
 */
#ifdef WIN32
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

#ifdef WIN32

	/*
	 * Retrieve handle passed here by pg_thrd_create() before allowing this
	 * thread to run.  (pg_thrd_current() can't use CurrentThread(), because
	 * that returns a pseudo-handle with the same value in all threads.)
	 */
	Assert(start_info->self);
	my_thrd_handle = start_info->self;
#endif

	free(start_info);

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
	pg_thrd_start_info *start_info;

	start_info = malloc(sizeof(*start_info));
	if (start_info == NULL)
		return pg_thrd_nomem;
	start_info->function = function;
	start_info->argument = argument;

#ifdef WIN32
	*thread = CreateThread(NULL, 0, pg_thrd_body, start_info,
						   CREATE_SUSPENDED, 0);
	if (*thread != NULL)
	{
		/*
		 * Give the thread its own handle so that pg_thrd_current() works,
		 * before it is allowed to start running.
		 */
		start_info->self = *thread;
		ResumeThread(*thread);

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

#ifdef WIN32
pg_thrd_t
pg_thrd_current_win32(void)
{
	/*
	 * This function is in .c file, to avoid potential cross-DLL complications
	 * if a load from thread_local is inlined.
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

#ifdef WIN32
/*-------------------------------------------------------------------------
 *
 * Windows implementation of tss_t functions.
 *
 * TlsAlloc() doesn't support destructors, and although FlsAlloc() does, it has
 * semantic differences that require some wrapping:
 *
 * 1.  We don't want to run destructors in tss_delete(), only on exit.
 * 2.  The calling convention for destructors is CALLBACK, so the function
 *     prototype doesn't match a plain function pointer.
 *
 * We use Windows' native TLS, but manage our own destructor table.  A single
 * dummy FLS is registered, and its destructor to drive our own from the
 * table.
 *
 *-------------------------------------------------------------------------
 */

typedef struct pg_tss_win32_entry
{
	pg_tss_t id;
	pg_tss_dtor_t destructor;
} pg_tss_win32_entry;

static pg_mtx_t pg_tss_win32_lock = PG_MTX_STATIC_INIT;
static int pg_tss_win32_count = 0;
static pg_tss_win32_entry pg_tss_win32_table[TLS_MINIMUM_AVAILABLE];
static DWORD pg_tss_win32_fls = FLS_OUT_OF_INDEXES;

/*
 * When the OS calls the destructor for our single dummy FLS, we need to call
 * the destructor for each TSS that has a destructor and a non-NULL value.
 */
static void CALLBACK
pg_tss_win32_call_destructors(void *dummy)
{
	/*
	 * XXX We don't yet have support for iterating more than once, in case
	 * destructors themselves cause more non-NULL values to appear.
	 */
	Assert(PG_TSS_DTOR_ITERATIONS == 1);

	pg_mtx_lock(&pg_tss_win32_lock);
	for (int i = 0; i < pg_tss_win32_count; ++i)
	{
		pg_tss_win32_entry *entry = &pg_tss_win32_table[i];
		void *value = pg_tss_get(entry->id);

		if (value)
		{
			pg_mtx_unlock(&pg_tss_win32_lock);

			pg_tss_set(entry->id, NULL);
			entry->destructor(value);

			pg_mtx_lock(&pg_tss_win32_lock);
		}
	}
	pg_mtx_unlock(&pg_tss_win32_lock);
}

int
pg_tss_win32_create(pg_tss_t *tss_id, pg_tss_dtor_t destructor)
{
	int result = pg_thrd_error;
	pg_tss_win32_entry *entry;

	/* If no destructor, just create a native TLS. */
	if (!destructor)
	{
		*tss_id = TlsAlloc();
		if (*tss_id != TLS_OUT_OF_INDEXES)
			result = pg_thrd_success;
		return result;
	}

	pg_mtx_lock(&pg_tss_win32_lock);
	/* Too many entries for our fixed-sized table? */
	if (pg_tss_win32_count == lengthof(pg_tss_win32_table))
		goto fail;
	/* Do we need to install the FLS destructor? */
	if (pg_tss_win32_fls == FLS_OUT_OF_INDEXES)
	{
		pg_tss_win32_fls = FlsAlloc(pg_tss_win32_call_destructors);
		if (pg_tss_win32_fls == FLS_OUT_OF_INDEXES)
			goto fail;
		FlsSetValue(pg_tss_win32_fls, (void *) 1);	/* non-NULL dummy */
	}
	*tss_id = TlsAlloc();
	if (*tss_id == TLS_OUT_OF_INDEXES)
		goto fail;
	entry = &pg_tss_win32_table[pg_tss_win32_count++];
	entry->id = *tss_id;
	entry->destructor = destructor;
	result = pg_thrd_success;
fail:
	pg_mtx_unlock(&pg_tss_win32_lock);

	return result;
}

void
pg_tss_win32_delete(pg_tss_t tss_id)
{
	pg_mtx_lock(&pg_tss_win32_lock);
	for (int i = 0; i < pg_tss_win32_count; ++i)
	{
		pg_tss_win32_entry *entry = &pg_tss_win32_table[i];
		if (entry->id == tss_id)
		{
			/* Move final slot into this slot. */
			if (i < pg_tss_win32_count - 1)
				pg_tss_win32_table[i] =
					pg_tss_win32_table[pg_tss_win32_count - 1];
			pg_tss_win32_count--;
			break;
		}
	}
	pg_mtx_unlock(&pg_tss_win32_lock);
}

#endif
