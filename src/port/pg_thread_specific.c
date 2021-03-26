#include "postgres.h"

#include "port/pg_thread_specific.h"

int
pg_thread_key_create(pg_thread_key_t *key)
{
#if defined(WIN32)
	if ((*key = TlsAlloc()) == TLS_OUT_OF_INDEXES)
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
#else
	return pthread_key_create(key, NULL);
#endif
}

int
pg_thread_key_delete(pg_thread_key_t key)
{
#if defined(WIN32)
	if (!TlsFree(key))
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
#else
	return pthread_key_delete(key);
#endif
}

int
pg_thread_setspecific(pg_thread_key_t key, const void *value)
{
#if defined(WIN32)
	if (!TlsSetValue(key, (void *) value))
	{
		_dosmaperr(GetLastError());
		return errno;
	}
	return 0;
#else
	return pthread_setspecific(key, value);
#endif
}

void *
pg_thread_getspecific(pg_thread_key_t key)
{
#if defined(WIN32)
	return TlsGetValue(key);
#else
	return pthread_getspecific(key);
#endif
}
