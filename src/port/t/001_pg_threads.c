#include "c.h"

#include "lib/pg_tap.h"
#include "port/pg_threads.h"

static pg_tss_t tss_with_destructor;
static pg_tss_t tss_without_destructor;

static int
thread_body_return(void *x)
{
	return (int)(intptr_t) x;
}

static int
thread_body_exit(void *x)
{
	pg_thrd_exit((int)(intptr_t) x);
	return 0;	/* not reached */
}

static int
thread_body_native_exit(void *x)
{
#ifdef WIN32
	ExitThread((DWORD)(intptr_t) x);
#else
	pthread_exit(x);
#endif
	return 0;	/* not reached */
}

static int
thread_body_tss_return(void *x)
{
	if (x)
	{
		pg_tss_set(tss_with_destructor, (void *) 1);
		pg_tss_set(tss_without_destructor, (void *) 2);
	}
	return 0;
}

static int
thread_body_tss_exit(void *x)
{
	if (x)
	{
		pg_tss_set(tss_with_destructor, (void *) 1);
		pg_tss_set(tss_without_destructor, (void *) 2);
	}
	pg_thrd_exit(0);
	return 0;	/* not reached */
}

static int
thread_body_tss_native_exit(void *x)
{
	if (x)
	{
		pg_tss_set(tss_with_destructor, (void *) 1);
		pg_tss_set(tss_without_destructor, (void *) 2);
	}

#ifdef WIN32
	ExitThread(0);
#else
	pthread_exit(0);
#endif
	return 0;	/* not reached */
}

static int
thread_body_capture_self(void *x)
{
	pg_thrd_t *handle = (pg_thrd_t *) x;
	*handle = pg_thrd_current();
	return 0;
}

static void *tss_destructor_received_value;
static int tss_destructor_call_count;

static void
tss_destructor(void *value)
{
	tss_destructor_call_count++;
	tss_destructor_received_value = value;
}

int
main()
{
	pg_thrd_error_t	error;
	pg_thrd_t	t;
	pg_thrd_t	t2;
	int	result;

	PG_BEGIN_TESTS();

	/* Check argument and return value for bodies that return. */
	error = pg_thrd_create(&t, thread_body_return, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 42);

	/* Check argument and return value for bodies that exit. */
	error = pg_thrd_create(&t, thread_body_exit, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 42);

	/* Check argument and return value for bodies that exit natively. */
	error = pg_thrd_create(&t, thread_body_native_exit, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 42);

	/* Create TSS IDs. */
	error = pg_tss_create(&tss_with_destructor, tss_destructor);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_tss_create(&tss_without_destructor, NULL);
	PG_EXPECT_EQ(error, pg_thrd_success);

	/* Check TSS destructor behavior for bodies that return. */
	tss_destructor_call_count = 0;
	tss_destructor_received_value = NULL;
	error = pg_thrd_create(&t, thread_body_tss_return, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 0);
	PG_EXPECT_EQ(tss_destructor_call_count, 1);
	PG_EXPECT(tss_destructor_received_value == (void *) 1);

	/* Check TSS destructor behavior for bodies that exit. */
	tss_destructor_call_count = 0;
	tss_destructor_received_value = NULL;
	error = pg_thrd_create(&t, thread_body_tss_exit, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 0);
	PG_EXPECT_EQ(tss_destructor_call_count, 1);
	PG_EXPECT(tss_destructor_received_value == (void *) 1);

	/* Check TSS destructor behavior for bodies that exit natively. */
	tss_destructor_call_count = 0;
	tss_destructor_received_value = NULL;
	error = pg_thrd_create(&t, thread_body_tss_native_exit, (void *) 42);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 0);
	PG_EXPECT_EQ(tss_destructor_call_count, 1);
	PG_EXPECT(tss_destructor_received_value == (void *) 1);

	/* Check pg_thrd_t comparison. */
	error = pg_thrd_create(&t, thread_body_capture_self, &t2);
	PG_EXPECT_EQ(error, pg_thrd_success);
	error = pg_thrd_join(t, &result);
	PG_EXPECT_EQ(error, pg_thrd_success);
	PG_EXPECT_EQ(result, 0);
	PG_EXPECT(pg_thrd_equal(t, t2));

	PG_END_TESTS();
}
