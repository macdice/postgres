#ifndef PG_THREAD_SPECIFIC_H
#define PG_THREAD_SPECIFIC_H

#if defined(WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif

#if defined(WIN32)
typedef DWORD pg_thread_key_t;
#else
typedef pthread_key_t pg_thread_key_t;
#endif

extern int pg_thread_key_create(pg_thread_key_t *key);
extern int pg_thread_key_delete(pg_thread_key_t key);
extern int pg_thread_setspecific(pg_thread_key_t key, const void *value);
extern void *pg_thread_getspecific(pg_thread_key_t key);

#endif
