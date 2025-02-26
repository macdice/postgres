/*-------------------------------------------------------------------------
 *
 * aio_controller.h
 *	  I/O depth control mechanism
 *
 * Plageman U concept controller
 *
 * IDENTIFICATION
 *	  src/include/storage/aio_controller.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AIO_CONTROLLER_H
#define AIO_CONTROLLER_H

#include "port/pg_bitutils.h"
#include "utils/timestamp.h"

/*
 * State for I/O depth controller.
 */
typedef struct AioController
{
	/* Current counters. */
	int16		depth;

	int64		probe_count;
	int16		probe_depth;
	uint64		latency_fast_avg;
	uint64		latency_slow_avg;
} AioController;

/*
 * Probe used to track an I/O.  User needs to store one with each I/O.
 */
typedef struct AioControllerProbe
{
	int16		depth;
	int64		time;
} AioControllerProbe;

static inline void
aio_controller_initialize(AioController *controller)
{
	memset(controller, 0, sizeof(*controller));
}

/*
 * Initialize a probe when an I/O starts.
 */
static inline void
aio_controller_start(AioController *controller, AioControllerProbe *probe)
{
	controller->depth++;
	probe->depth = controller->depth;
	probe->time = GetCurrentTimestamp();
}

/*
 * Called when an I/O is about to be waited for.  Returns recommended change
 * to I/O depth.
 */
static inline int
aio_controller_wait(AioController *controller, AioControllerProbe *probe)
{
	uint64		latency = GetCurrentTimestamp() - probe->time;
	int			change = 0;


	Assert(controller->depth > 0);
	controller->depth--;

	/* Base case for empty queue. */
	if (controller->depth == 0)
		return 1;

	if (controller->probe_depth != probe->depth)
	{
		/* We're getting back probes for a level change.  Clobber stats. */
		controller->probe_depth = probe->depth;
		controller->probe_count = 1;
		controller->latency_slow_avg = latency;
		controller->latency_fast_avg = latency;
	}
	else
	{
		controller->probe_count++;
		controller->latency_slow_avg -= controller->latency_slow_avg / 8;
		controller->latency_slow_avg += latency / 8;
		controller->latency_fast_avg -= controller->latency_slow_avg / 2;
		controller->latency_fast_avg += latency / 2;
	}

	/* Enough data to decide yet? */
	if (controller->probe_count >= 3)
	{
		if (controller->latency_fast_avg > controller->latency_slow_avg &&
			probe->depth == controller->depth)
		{
			/*
			 * Latency going up, which is bad.  If we haven't already
			 * decreased from the probe's depth, do it now.
			 */
			change = -1;
		}
		else if (controller->latency_fast_avg < controller->latency_slow_avg &&
				 probe->depth == controller->depth)
		{
			/*
			 * Latency going down, which is goo.  If we haven't already
			 * increased from the probe's depth, do it now.
			 */
			change = 1;
		}
	}


#define INSTRUMENT
#ifdef INSTRUMENT
	{
		char		line[1024];

		for (int i = 0; i < controller->depth; i++)
		{
			line[i] = '.';
		}
		line[controller->depth] = '\0';
		elog(DEBUG1,
			 "latency=%zuus %c decision=%c %s",
			 controller->latency_slow_avg,
			 controller->latency_fast_avg > controller->latency_slow_avg ? '^' : 'v',
			 change == 0 ? '-' : change > 0 ? '^' : 'v',
			 line);
	}
#endif

	return change;
}

#endif
