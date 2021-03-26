/*
 * A POSIX-style wrappers for a subset of Windows and POSIX threads.
 */

#ifndef PG_THREAD_H
#define PG_THREAD_H

#ifdef WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#ifdef WIN32
struct pg_thread_win32_thunk;
typedef struct pg_thread_win32_thunk *pg_thread_t;
#else
#include <pthread.h>
typedef pthread_t pg_thread_t;
#endif

extern int pg_thread_create(pg_thread_t *thread, void *(*func)(void *), void *arg);
extern int pg_thread_join(pg_thread_t thread, void **retval);
extern int pg_thread_equal(pg_thread_t t1, pg_thread_t t2);
extern pg_thread_t pg_thread_self(void);
extern void pg_thread_exit(void *value);

#endif
