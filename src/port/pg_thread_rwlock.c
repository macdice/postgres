#include "postgres.h"

#include "port/pg_thread_rwlock.h"

int
pg_thread_rwlock_init(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	/* XXX not good for any current purpose, as these don't work across processes */
	InitializeSRWLock(&rwlock->srw);
	rwlock->wr_held = false;
	return 0;
#else
	return pthread_rwlock_init(rwlock, NULL);
#endif
}

int
pg_thread_rwlock_destroy(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	/* nothing to do */
	return 0;
#else
	return pthread_rwlock_destroy(rwlock);
#endif
}

int
pg_thread_rwlock_rdlock(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	AcquireSRWLockExclusive(&rwlock->srw);
	return 0;
#else
	return pthread_rwlock_rdlock(rwlock);
#endif
}

int
pg_thread_rwlock_wrlock(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	AcquireSRWLockExclusive(&rwlock->srw);
	rwlock->wr_held = true;
	return 0;
#else
	return pthread_rwlock_wrlock(rwlock);
#endif
}

int
pg_thread_rwlock_tryrdlock(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	if (!TryAcquireSRWLockExclusive(&rwlock->srw))
		return EBUSY;
	return 0;
#else
	return pthread_rwlock_tryrdlock(rwlock);
#endif
}

int
pg_thread_rwlock_trywrlock(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	if (!TryAcquireSRWLockExclusive(&rwlock->srw))
		return EBUSY;
	rwlock->wr_held = true;
	return 0;
#else
	return pthread_rwlock_trywrlock(rwlock);
#endif
}

int
pg_thread_rwlock_unlock(pg_thread_rwlock_t *rwlock)
{
#if defined(WIN32)
	if (rwlock->wr_held)
	{
		ReleaseSRWLockExclusive(&rwlock->srw);
		rwlock->wr_held = false;
	}
	else
		ReleaseSRWLockShared(&rwlock->srw);
	return 0;
#else
	return pthread_rwlock_unlock(rwlock);
#endif
}
