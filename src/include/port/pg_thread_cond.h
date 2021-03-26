/*
 * A POSIX-style wrappers for a subset of POSIX condvars.
 *
 * This is a minimal subset, primarily provided to support
 * pg_thread_barrier.c.  Therefore it's not needed on Windows for now.
 */

#ifndef PG_THREAD_COND_H
#define PG_THREAD_COND_H

#include "port/pg_thread_mutex.h"

#if defined(WIN32)
#error "not implemented for Windows"
#else
#include <pthread.h>
typedef pthread_cond_t pg_thread_cond_t;
#endif

extern int pg_thread_cond_init(pg_thread_cond_t *cond);
extern int pg_thread_cond_destroy(pg_thread_cond_t *cond);
extern int pg_thread_cond_signal(pg_thread_cond_t *cond);
extern int pg_thread_cond_broadcast(pg_thread_cond_t *cond);
extern int pg_thread_cond_wait(pg_thread_cond_t *cond,
							   pg_thread_mutex_t *mutex);
extern int pg_thread_cond_timedwait(pg_thread_cond_t *cond,
									pg_thread_mutex_t *mutex,
									struct timespec *timeout);

#endif
