#include "port/pg_threads.h"

#include <errno.h>
#include <stdlib.h>

/*
 * Becaus the C11 and Pthread function types don't match (different return
 * types), and we don't want to bet that it's safe to cast between them, we'll
 * use an intermediate thunk object.
 */
typedef struct pg_thrd_thunk
{
	pg_thrd_start_t function;
	void *argument;
	int result;
} pg_thrd_thunk;

static pg_tss_t my_thunk_key;
static pg_once_flag my_thunk_key_once = PG_ONCE_FLAG_INIT;

static void
my_thunk_key_init(void)
{
	pg_tss_create(&my_thunk_key, NULL);
}

static void *
pg_thrd_trampoline(void *vthunk)
{
	pg_thrd_thunk *thunk = (pg_thrd_thunk *) vthunk;

	pg_call_once(&my_thunk_key_once, my_thunk_key_init);

	/* Allow pg_thrd_exit() to find thunk, to set the result.  */
	pg_tss_set(my_thunk_key, thunk);

	/* Run user function. */
	thunk->result = thunk->function(thunk->argument);

	/* Allow pg_thrd_join() to receive the thunk. */
	return thunk;
}

int
pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument)
{
	pthread_attr_t attr;
	pg_thrd_thunk *thunk;
	int error;

	thunk = malloc(sizeof(*thunk));
	if (thunk == NULL)
		return pg_thrd_nomem;
	thunk->function = function;
	thunk->argument = argument;

	if (pthread_attr_init(&attr) != 0)
		return pg_thrd_error;
	error = pthread_create(thread, &attr, pg_thrd_trampoline, thunk);
	if (error == 0)
		return pg_thrd_success;
	else if (error == ENOMEM)
		return pg_thrd_nomem;
	else
		return pg_thrd_error;
}

int
pg_thrd_join(pg_thrd_t thread, int *result)
{
	void *vthunk;
	int error;

	if (pthread_join(thread, &vthunk) != 0)
	{
		pg_thrd_thunk *thunk = (pg_thrd_thunk *) vthunk;

		if (result)
			*result = thunk->result;
		error = pg_thrd_success;
	}
	else
	{
		error = pg_thrd_error;
	}
	free(vthunk);

	return error;
}

void
pg_thrd_exit(int result)
{
	pg_thrd_thunk *thunk = (pg_thrd_thunk *) pg_tss_get(my_thunk_key);

	thunk->result = result;

	pthread_exit(thunk);
}
