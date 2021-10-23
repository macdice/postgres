/*
 * POSIX-style wrappers for a subset of POSIX rwlocks.
 */

#ifndef PG_THREAD_RWLOCK_H
#define PG_THREAD_RWLOCK_H

#if defined(WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif

#if defined(WIN32)
typedef CRITICAL_SECTION pg_thread_mutex_t; /* FIXME */
#else
typedef pthread_rwlock_t pg_thread_rwlock_t;
#endif

extern int pg_thread_rwlock_init(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_destroy(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_rdlock(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_wrlock(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_tryrdlock(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_trywrlock(pg_thread_rwlock_t *rwlock);
extern int pg_thread_rwlock_unlock(pg_thread_rwlock_t *rwlock);

#endif
