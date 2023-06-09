/*-------------------------------------------------------------------------
 *
 * Similar to pthread_barrier_t, but based on standard C threads.h.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_THREAD_BARRIER_H
#define PG_THREAD_BARRIER_H

#include <threads.h>

#define PG_THREAD_BARRIER_SERIAL_THREAD (-1)

typedef struct pg_thread_barrier
{
	bool		sense;			/* we only need a one bit phase */
	int			count;			/* number of threads expected */
	int			arrived;		/* number of threads that have arrived */
	mtx_t		mutex;
	cnd_t		cond;
} pg_thread_barrier_t;

extern int	pg_thread_barrier_init(pg_thread_barrier_t *barrier,
								   int count);
extern int	pg_thread_barrier_wait(pg_thread_barrier_t *barrier);
extern int	pg_thread_barrier_destroy(pg_thread_barrier_t *barrier);

#endif
