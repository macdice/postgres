#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/eventflag.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procnumber.h"

#define EVENTFLAG_FIRED			0x80000000
#define EVENTFLAG_WAITER		0x40000000
#define EVENTFLAG_GRANTED		0x20000000
#define EVENTFLAG_PROCNO_MASK	0x0fffffff

/*
 * Initialize an EventFlag.
 */
void
eventflag_init(EventFlag *flag)
{
	pg_atomic_init_u32(&flag->word, 0);
}

/*
 * Wait for the event to fire.  If someone is already waiting or has been
 * granted the event exclusively then give up immediately and return false.  If
 * the event has fired already, return true, and otherwise wait until it fires
 * and return true.
 */
bool
eventflag_wait_exclusive(EventFlag *flag, uint32 wait_event_info)
{
	uint32		word = pg_atomic_read_u32(&flag->word);

	for (;;)
	{
		/* Has it already fired? */
		if (word & EVENTFLAG_FIRED)
		{
			if (!(word & EVENTFLAG_GRANTED))
			{
				/*
				 * Not already granted.  Try to grant to self.  This is the
				 * common case when someone waits for something uncontended
				 * that has already happened, for example IO completions
				 * issued far enough ahead of time to avoid IO stalls.
				 */
				Assert(!(word & EVENTFLAG_WAITER));
				if (!pg_atomic_compare_exchange_u32(&flag->word,
													&word,
													EVENTFLAG_FIRED |
													EVENTFLAG_GRANTED |
													MyProcNumber))
					continue;
				pg_read_barrier();
				return true;
			}
			else if ((word & EVENTFLAG_PROCNO_MASK) == MyProcNumber)
			{
				/* Granted to me by eventflag_fire(). */
				pg_read_barrier();
				return true;
			}
			else
			{
				/* Granted to someone else, so give up. */
				break;
			}
		}

		/* Is someone already waiting for it to fire? */
		if (word & EVENTFLAG_WAITER)
		{
			if ((word & EVENTFLAG_PROCNO_MASK) != MyProcNumber)
				break;		/* yes and it's not me, give up */
		}
		else
		{
			/* Try to become the waiter. */
			if (!pg_atomic_compare_exchange_u32(&flag->word,
												&word,
												EVENTFLAG_WAITER | MyProcNumber))
				continue;
		}

		/* Wait for eventflag_fire() to wake us up. */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1, wait_event_info);
		ResetLatch(MyLatch);
		word = pg_atomic_read_u32(&flag->word);
	}
	return false;
}

/*
 * Without waiting, try to become the waiter for this event.  Return false if
 * the event has already fired or there is already a waiter.
 */
bool
eventflag_begin_wait_exclusive(EventFlag *flag)
{
	uint32		word = pg_atomic_read_u32(&flag->word);

	for (;;)
	{
		/* Has it already fired or being waited on? */
		if (word & (EVENTFLAG_FIRED | EVENTFLAG_WAITER))
			return false;

		/* Try to become the waiter. */
		if (!pg_atomic_compare_exchange_u32(&flag->word,
											&word,
											EVENTFLAG_WAITER | MyProcNumber))
			continue;
		return true;
	}
	return false;
}

/*
 * Check if the event has fired and been granted.
 */
bool
eventflag_has_fired(EventFlag *flag)
{
	uint32		word = pg_atomic_read_u32(&flag->word);

	return word & EVENTFLAG_FIRED;
}

/*
 * Fire an event, releasing any backend that is waiting in
 * eventflag_wait_exclusive().  Can be called in an asynchronous signal handler
 * or a Windows helper thread.
 */
void
eventflag_fire(EventFlag *flag)
{
	pg_write_barrier();

	for (;;)
	{
		uint32		word = pg_atomic_read_u32(&flag->word);

		Assert(!(word & EVENTFLAG_FIRED));

		if (word & EVENTFLAG_WAITER)
		{
			/* There is a waiter.  Mark as fired and granted, and wake it up. */
			if (!pg_atomic_compare_exchange_u32(&flag->word,
												&word,
												EVENTFLAG_FIRED |
												EVENTFLAG_GRANTED |
												(word & EVENTFLAG_PROCNO_MASK)))
				continue;
			SetLatch(&GetPGProcByNumber(word & EVENTFLAG_PROCNO_MASK)->procLatch);
		}
		else
		{
			/* No waiter.  Just mark as fired. */
			if (!pg_atomic_compare_exchange_u32(&flag->word,
												&word,
												EVENTFLAG_FIRED))
				continue;
		}
		break;
	}
}
