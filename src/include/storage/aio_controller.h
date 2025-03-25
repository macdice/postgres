/*-------------------------------------------------------------------------
 *
 * aio_controller.h
 *	  I/O depth control mechanism
 *
 * The algorithm has two goals:
 *
 * 1.  Anti-flood: Don't increase depth if that also increases latency,
 * revealing that I/Os are queuing up somewhere.  Seek the level where the
 * correlation reverses, ie the current limit of the hardware.
 *
 * 2.  Anti-stall: As long as anti-flood isn't activate, control I/O depth
 * continuously as required to keep the average done I/O count high enough to
 * avoid double the variance we've recently seen.  As an emergency override,
 * if it does drop below half, abandon all fancy notions and double the depth.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * The number of samples of depth and latency traveling in the same direction
 * that constitute evidence that I/O requests are queueing up somewhere, and
 * we should decrease the depth.
 */
#define LATENCY_CORRELATION_LIMIT 2

/* Fixed-point arithmetic type, half of it used for a binary fraction. */
typedef int32_t fixed32;
#define FIXED32_SCALE (1 << 16)
#define make_fixed32(i) ((i) * FIXED32_SCALE)
#define fixed32_to_int(f) (((f) + FIXED32_SCALE / 2) / FIXED32_SCALE)
#define fixed32_to_double(f) ((double) f / (double) FIXED32_SCALE)

/*
 * State for I/O depth controller.
 */
typedef struct AioController
{
	/* Current counters. */
	int16		depth;
	int16		done;

	/* Anti-stall statistics and PID state. */
	int16		max_abs_dev_hand;
	int16		max_abs_dev[4];

	
	int16		min_done;
	fixed32		avg_done;
	fixed32		control_variable;
	fixed32		control_variable_saturated;
	fixed32		integral;
	fixed32		derivative;
	fixed32		error;

	/* Anti-flood statistics. */
	bool		latency_monitoring;
	int64		probe_latency;
	int16		probe_depth;
	int16		correlation;
	int16		correlation_delta;
} AioController;

/*
 * Probe used to track an I/O.  User needs to store one with each I/O.
 */
typedef struct AioControllerProbe
{
	bool		done;
	int16		depth;
	fixed32		error;
	int64		time;
} AioControllerProbe;

/*
 * Exponential moving average, alpha = 0.125.
 */
static inline void
fixed32_avg(fixed32 *avg, fixed32 value)
{
	*avg -= *avg / 8;
	*avg += value / 8;
}

/*
 * Exponential moving average, alpha = 0.5.
 */
static inline void
fixed32_avg_fast(fixed32 *avg, fixed32 value)
{
	*avg -= *avg / 2;
	*avg += value / 2;
}

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
	probe->done = false;
	probe->depth = controller->depth;
	probe->time = controller->latency_monitoring ? GetCurrentTimestamp() : 0;
}

static inline void
aio_controller_analyze_latency(AioController *controller, AioControllerProbe *probe)
{
	int64		latency;

	Assert(probe->time != 0);

	/* This probe carries a start time.  Compute the correlation. */
	latency = GetCurrentTimestamp() - probe->time;

	/* First probe done since monitoring enabled, no comparison yet. */
	if (controller->probe_latency == 0)
	{
		controller->probe_depth = probe->depth;
		controller->probe_latency = latency;
		controller->correlation = 0;
		return;
	}

	/* No comparison without change. */
	if (probe->depth == controller->probe_depth)
		return;

	/* Did depth and latency change in the same direction? */
	controller->correlation_delta =
		(latency > controller->probe_latency) ==
		(probe->depth > controller->probe_depth) ? 1 : -1;
	controller->correlation += controller->correlation_delta;
	controller->correlation = Min(LATENCY_CORRELATION_LIMIT,
								  Max(-LATENCY_CORRELATION_LIMIT,
									  controller->correlation));
	controller->probe_latency = latency;
}

/*
 * Report that an I/O has physically completed, but not yet been waited for.
 * This should be called when pgaio_wref_check_done returns true to indicate
 * that a wait would not stall, if ever.
 */
static inline void
aio_controller_done(AioController *controller, AioControllerProbe *probe)
{
	Assert(!probe->done);
	probe->done = true;
	controller->done++;
	Assert(controller->done <= controller->depth);
	if (probe->time != 0)
		aio_controller_analyze_latency(controller, probe);
	controller->probe_depth = probe->depth;
}

/*
 * Called when an I/O is about to be waited for.  Returns recommended change
 * to I/O depth.
 */
static inline int
aio_controller_wait(AioController *controller, AioControllerProbe *probe)
{
	fixed32		half_avg_done;
	int16		change;
	enum
	{
		MODE_ANTISTALL,
		MODE_PID,
		MODE_ANTIFLOOD
	}			mode;

	/* Process the probe and analyze latency if done() wasn't called. */
	Assert(controller->depth > 0);
	controller->depth--;
	Assert(!controller->done || controller->done > 0);
	if (probe->done)
		controller->done--;
	else if (probe->time != 0)
		aio_controller_analyze_latency(controller, probe);
	if (probe->done)
		controller->probe_depth = probe->depth;
	Assert(controller->done <= controller->depth);

	/* Base case for empty queue. */
	if (controller->depth == 0)
		return 2;

	/*
	 * If the done I/O counter drop below half its recent average, we panic
	 * and double the I/O depth.
	 *
	 * Otherwise we use a standard closed loop control algorithm to tune the
	 * depth as required to keep the average number of done I/Os at 2.125 *
	 * the lowest number number of done I/Os we've seen recently.
	 *
	 * The idea is to push the average number of done I/Os up so high that we
	 * never see it drop below half so you can count on at least half at all
	 * times.
	 */
	fixed32_avg(&controller->avg_done, make_fixed32(controller->done));
	half_avg_done = controller->avg_done / 2;

	/*
	 * Track maximum absolute deviation from the mean in rotating buckets.
	 * Values live long enough to remember events outside two standard
	 * deviations (though we don't expect normal distribution).
	 */
	abs_dev = fixedcontroller->avg_done - make_fixed32(controller->done);
	if (abs_dev > 0)
	{
		fixed32 max_abs_dev = 0;
		uint8 hand = controller->max_abs_dev_hand++;
		uint8 tick = hand % 32;
		uint8 word = (tick / 32) % lengthof(controller->max_abs_dev);

		if (time == 0 || controller->max_abs_dev[word] < abs_dev)
			controller->max_abs_dev[word] = abs_dev;
		for (int i = 0; i < lengthof(controller->max_abs_dev); ++i)
			max_abs_dev = Max(max_abs_dev, controller->max_abs_dev[i]);
		if (min_window == 0)
			controller->min_done = 0;
		else
			controller->min_done = pg_rightmost_one_pos64(min_window) + 1;
	}
		
	/* Boost if in danger, otherwise try to maintain steady state. */
	if (controller->done < fixed32_to_int(half_avg_done))
	{
		/*
		 * Current done I/O counter is dangerously low: take evasive action.
		 * Rather than doubling the *current* depth, double the depth since
		 * the last probe came back, unless it was higher that the depth we're
		 * currently using.  (XXX?)  This prevents repeated overshoot with a
		 * very deep queue.
		 */
		change = Min(controller->depth, probe->depth * 2);
		controller->lowest_recent_done = make_fixed32(controller->done);
		mode = MODE_ANTISTALL;
	}
	else
	{
		const int	Kp = 2;		/* Kp = 2.0 */
		const int	Ri = 2;		/* Ki = 0.5 */
		const int	Rd = 2;		/* Kd = 0.5 */
		const int	Raw = 8;	/* Kaw = 0.125 */
		const int	Rt = 8;		/* T = 0.125 */
		const fixed32 min = make_fixed32(-3) / 2;
		const fixed32 max = make_fixed32(3) / 2;
		fixed32		setpoint,
					control_variable,
					process_variable,
					p,
					i,
					d,
					error;

		/* The avg_done we want and the avg_done we have. */
		setpoint = controller->avg_done - lowest_recent_done * 2;
		setpoint += setpoint / 8;	/* a bit more, avoid boost threshold */
		process_variable = controller->avg_done;

		/* PID with anti-windup, using only add/sub and shifts. */
		error = setpoint - process_variable;
		p = error * Kp;
		controller->integral += error / Rt;
		controller->integral += (controller->control_variable_saturated -
								 controller->control_variable) / Raw;
		i = controller->integral / Ri;
		controller->derivative = (error - controller->error) * Rt;
		d = controller->derivative / Rd;
		control_variable = p + i + d;
		controller->control_variable = control_variable;
		if (control_variable < min)
			control_variable = min;
		if (control_variable > max)
			control_variable = max;
		controller->control_variable_saturated = control_variable;
		controller->error = error;

		change = fixed32_to_int(control_variable);
		mode = MODE_PID;
	}

	/* Turn on latency monitoring if we are going up. */
	if (change > 0 && !controller->latency_monitoring)
	{
		controller->latency_monitoring = true;
		controller->probe_latency = 0;
	}

	/* Override if repeated positive latency correlation observed. */
	if (controller->correlation == LATENCY_CORRELATION_LIMIT)
	{
		/*
		 * System out of juice.  Anti-flood behavior overrides earlier
		 * decision.  Drift down in search of the level that reverses the
		 * correlation.
		 */
		change = -1;
		mode = MODE_ANTIFLOOD;
	}
	else if (controller->correlation == LATENCY_CORRELATION_LIMIT - 1 &&
			 controller->correlation_delta == -1)
	{
		/*
		 * Correlation has begun to reverse, but we don't fully trust it yet.
		 * Drift up if we were trying to go up, damping any boost decision, so
		 * that we oscillate around the correlation change depth, if it
		 * persists.  A continuing search is needed so we can notice if the
		 * system behavior changes (eg I/O workers start up, other users stop
		 * flooding storage, etc).
		 */
		if (change > 1)
		{
			change = 1;
			mode = MODE_ANTIFLOOD;
		}
	}
	else if (controller->correlation == -LATENCY_CORRELATION_LIMIT &&
			 controller->correlation_delta == -1)
	{
		/*
		 * There is a well established history of negative correlation, so
		 * turn the overheads off as soon as we've confirmed it, or
		 * reconfirmed it just once.  This system is keeping up without
		 * trouble for now.
		 */
		controller->latency_monitoring = false;
	}

#define INSTRUMENT
#ifdef INSTRUMENT
	{
		char		line[1024];

		for (int i = 0; i < controller->depth; i++)
		{
			if (i <= controller->done)
				line[i] = 'o';	/* a done I/O */
			else
				line[i] = '.';	/* an in-flight I/O */
			if (i == fixed32_to_int(controller->lowest_recent_done))
				line[i] = 'l';	/* lowest recent done level */
			if (i == fixed32_to_int(controller->avg_done))
				line[i] = 'A';	/* average done count */
			if (i == fixed32_to_int(half_avg_done))
				line[i] = 'H';	/* half average done count */
		}
		line[controller->depth] = '\0';
		elog(DEBUG1,
			 "controller=%s change=%+d %s",
			 mode == MODE_ANTISTALL ? "antistall" :
			 mode == MODE_PID ? "      pid" :
			 "antiflood",
			 change,
			 line);
	}
#endif

	return change;
}

#endif
