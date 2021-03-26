/*
 * A POSIX-style wrappers for Windows and POSIX thread barriers.
 */

#ifndef PG_THREAD_BARRIER_H
#define PG_THREAD_BARRIER_H

#ifdef WIN32
#include <windows.h>
#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)
#include "port/pg_thread_cond.h"
#include "port/pg_thread_mutex.h"
#else
#include <pthread.h>
#endif

#if defined(WIN32)
typedef SYNCHRONIZATION_BARRIER pg_thread_barrier_t;
#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)
typedef struct pg_thread_barrier
{
	bool		sense;			/* we only need a one bit phase */
	int			count;			/* number of threads expected */
	int			arrived;		/* number of threads that have arrived */
	pg_thread_mutex_t mutex;
	pg_thread_cond_t cond;
} pg_thread_barrier_t;
#else
typedef pthread_barrier_t pg_thread_barrier_t;
#endif

#ifdef PTHREAD_BARRIER_SERIAL_THREAD
#define PG_THREAD_BARRIER_SERIAL_THREAD PTHREAD_BARRIER_SERIAL_THREAD
#else
#define PG_THREAD_BARRIER_SERIAL_THREAD (-1)
#endif

extern int pg_thread_barrier_init(pg_thread_barrier_t *barrier, int count);
extern int pg_thread_barrier_wait(pg_thread_barrier_t *barrier);
extern int pg_thread_barrier_destroy(pg_thread_barrier_t *barrier);

#endif
