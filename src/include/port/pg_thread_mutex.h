/*
 * A POSIX-style wrappers for a subset of POSIX mutexes.
 */

#ifndef PG_THREAD_MUTEX_H
#define PG_THREAD_MUTEX_H

#if defined(WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif

#if defined(WIN32)
typedef CRITICAL_SECTION pg_thread_mutex_t;
#else
typedef pthread_mutex_t pg_thread_mutex_t;
#endif

extern int pg_thread_mutex_init(pg_thread_mutex_t *mutex);
extern int pg_thread_mutex_destroy(pg_thread_mutex_t *mutex);
extern int pg_thread_mutex_lock(pg_thread_mutex_t *mutex);
extern int pg_thread_mutex_unlock(pg_thread_mutex_t *mutex);

#endif
