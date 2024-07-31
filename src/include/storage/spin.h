/*-------------------------------------------------------------------------
 *
 * spin.h
 *	   API for spinlocks.
 *
 *
 *	The interface to spinlocks is defined by the typedef "slock_t" and
 *	these macros:
 *
 *	void SpinLockInit(volatile slock_t *lock)
 *		Initialize a spinlock (to the unlocked state).
 *
 *	void SpinLockAcquire(volatile slock_t *lock)
 *		Acquire a spinlock, waiting if necessary.
 *		Time out and abort() if unable to acquire the lock in a
 *		"reasonable" amount of time --- typically ~ 1 minute.
 *		Acquire (including read barrier) semantics.
 *
 *	void SpinLockRelease(volatile slock_t *lock)
 *		Unlock a previously acquired lock.
 *		Release (including write barrier) semantics.
 *
 *	bool SpinLockFree(slock_t *lock)
 *		Tests if the lock is free. Returns true if free, false if locked.
 *		This does *not* change the state of the lock.
 *
 *	Callers must beware that the macro argument may be evaluated multiple
 *	times!
 *
 *	Load and store operations in calling code are guaranteed not to be
 *	reordered with respect to these operations, because they include a
 *	compiler barrier.  (Before PostgreSQL 9.5, callers needed to use a
 *	volatile qualifier to access data protected by spinlocks.)
 *
 *	Keep in mind the coding rule that spinlocks must not be held for more
 *	than a few instructions.  In particular, we assume it is not possible
 *	for a CHECK_FOR_INTERRUPTS() to occur while holding a spinlock, and so
 *	it is not necessary to do HOLD/RESUME_INTERRUPTS() in these macros.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/spin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPIN_H
#define SPIN_H

#ifdef FRONTEND
#error "spin.h may not be included from frontend code"
#endif

#include "port/atomics.h"

/* Support for dynamic adjustment of spins_per_delay */
#define DEFAULT_SPINS_PER_DELAY  100

typedef pg_atomic_flag slock_t;

/*
 * Support for spin delay which is useful in various places where
 * spinlock-like procedures take place.
 */
typedef struct
{
	int			spins;
	int			delays;
	int			cur_delay;
	const char *file;
	int			line;
	const char *func;
} SpinDelayStatus;

static inline void
init_spin_delay(SpinDelayStatus *status,
				const char *file, int line, const char *func)
{
	status->spins = 0;
	status->delays = 0;
	status->cur_delay = 0;
	status->file = file;
	status->line = line;
	status->func = func;
}

#define init_local_spin_delay(status) init_spin_delay(status, __FILE__, __LINE__, __func__)
extern void perform_spin_delay(SpinDelayStatus *status);
extern void finish_spin_delay(SpinDelayStatus *status);
extern void set_spins_per_delay(int shared_spins_per_delay);
extern int	update_spins_per_delay(int shared_spins_per_delay);

/* Out-of-line part of spinlock acquisition. */
extern int	s_lock(volatile slock_t *lock,
				   const char *file, int line,
				   const char *func);

static inline void
SpinLockInit(volatile slock_t *lock)
{
	pg_atomic_init_flag(lock);
}

#define SpinLockAcquire(lock)						\
	(pg_atomic_test_set_flag(lock) ? 0 :			\
	 s_lock((lock), __FILE__, __LINE__, __func__))

static inline void
SpinLockRelease(volatile slock_t *lock)
{
	/*
	 * Use a relaxed load to see that it's currently held.  That's OK because
	 * we expect the calling thread to be the one that set it.
	 */
	Assert(!pg_atomic_unlocked_test_flag(lock));

	pg_atomic_clear_flag(lock);
}

#endif							/* SPIN_H */
