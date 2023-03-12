/*-------------------------------------------------------------------------
 *
 * pgsleep.c
 *	   Portable delay handling.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * src/port/pgsleep.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <time.h>

/*
 * In a Windows backend, we don't use this implementation, but rather
 * the signal-aware version in src/backend/port/win32/signal.c.
 */
#if defined(FRONTEND) || !defined(WIN32)

/*
 * pg_usleep --- delay the specified number of microseconds.
 *
 * NOTE: although the delay is specified in microseconds, the effective
 * resolution is only 1/HZ on systems that use periodic kernel ticks to wake
 * up.  This may cause sleeps to be rounded up by 1-20 milliseconds on older
 * Unixen and Windows.
 *
 * On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
 *
 * CAUTION: if interrupted by a signal, this function will return, but its
 * interface doesn't report that.  It's not a good idea to use this
 * for long sleeps in the backend, because backends are expected to respond to
 * interrupts promptly.  Better practice for long sleeps is to use WaitLatch()
 * with a timeout.
 */
void
pg_usleep(long microsec)
{
	if (microsec > 0)
	{
#ifndef WIN32
		struct timespec delay;

		delay.tv_sec = microsec / 1000000L;
		delay.tv_nsec = (microsec % 1000000L) * 1000;
		(void) nanosleep(&delay, NULL);
#else
		SleepEx((microsec < 500 ? 1 : (microsec + 500) / 1000), FALSE);
#endif
	}
}

#endif							/* defined(FRONTEND) || !defined(WIN32) */
