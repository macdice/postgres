/*
 * A POSIX-style wrappers for a subset of POSIX once.
 */

#ifndef PG_THREAD_ONCE_H
#define PG_THREAD_ONCE_H

#if defined(WIN32)
#include <windows.h>
#include "port/pg_thread_mutex.h"
typedef struct pg_thread_once_t {
	bool initialized;
	pg_thread_mutex_t mutex;
} pg_thread_once_t;
#define PG_THREAD_ONCE_INIT {false, PG_THREAD_MUTEX_INITIALIZER}
#else
#include <pthread.h>
typedef pthread_once_t pg_thread_once_t;
#define PG_THREAD_ONCE_INIT PTHREAD_ONCE_INIT
#endif

extern int pg_thread_once(pg_thread_once_t *once_control, void (*init_routine)(void));

#endif
