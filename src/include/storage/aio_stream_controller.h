#ifndef AIO_STREAM_CONTROLLER_H
#define AIO_STREAM_CONTROLLER_H

#include <math.h>
#include <fcntl.h>

#include "lib/pg_fixedpoint.h"

/*
 * Minimum levels used at bootstrap and later as minimums, though not if
 * max_ios is set lower.
 */
#define AIO_SC_MIN_IO_TARGET 2
#define AIO_SC_MIN_IO_DONE_TARGET 2

/*
 * Smoothing constants used for exponential weighted moving statistics for
 * ios_done, represented as 1-(1/K) to allow bitshifting.
 */
#define AIO_SC_EWMA_ALPHA 256
#define AIO_SC_EWMV_LAMBDA 256

/* Gain constants used for PID controller. */
#define AIO_SC_PID_KP 2
#define AIO_SC_PID_KI 0
#define AIO_SC_PID_KD 0
#define AIO_SC_PID_KAW 0

typedef struct AioStreamController
{
	/* Statistics used to compute ios_done_target. */
	pg_fixed32	ios_done_ewma;
	pg_fixed32	ios_done_ewmv;
	pg_fixed32	ios_done_ewmv_recompute_lower;
	pg_fixed32	ios_done_ewmv_recompute_upper;
	pg_fixed32	ios_done_ewma_recompute_lower;
	pg_fixed32	ios_done_ewma_recompute_upper;
	pg_fixed32	ios_done_target;

	/* PID controller state for driving ios_done towards it ios_target. */
	pg_fixed32	pid_integral;
	pg_fixed32	pid_derivative;
	pg_fixed32	pid_control_variable;
	pg_fixed32	pid_control_variable_saturated;
	pg_fixed32	pid_error;
} AioStreamController;

/*
 * https://en.wikipedia.org/wiki/Proportional-integral-derivative_controller
 *
 * Gain constants should be positive integers, or negative integers which are
 * interpreted as 1/-n.  Kp = 2 simply means 2, but Kp = -2 means 1/2.
 * Intended to be inlined as bitshifts.  Anti-windup is needed to support
 * clamping.
 *
 * XXX A filtered derivative might deal better with jitter, if it could be
 * done without non-constant division.  Perhaps moving average with shifts?
 */
static inline pg_fixed32
aio_stream_controller_pid(AioStreamController *state,
						  int Kp,
						  int Ki,
						  int Kd,
						  int Kaw,
						  pg_fixed32 min,
						  pg_fixed32 max,
						  pg_fixed32 setpoint,
						  pg_fixed32 process_variable)
{
	pg_fixed32	control_variable;
	pg_fixed32	error;
	pg_fixed32	p;
	pg_fixed32	i;
	pg_fixed32	d;

	/* How far are we from the setpoint? */
	error = setpoint - process_variable;

	/* Proportional component, for the main correction. */
	if (Kp >= 0)
		p = pg_fixed32_mul(error, pg_fixed32_from_double(0.2));// * Kp;
//		p = error * Kp;
	else
		p = error / -Kp;

	/* Integral component for accumulating error with anti-windup. */
	if (Ki >= 0)
		state->pid_integral += pg_fixed32_mul(error, pg_fixed32_from_double(0.2)); // * Ki;
	else
		state->pid_integral += error / -Ki;
	if (Kaw >= 0)
		state->pid_integral += pg_fixed32_mul(state->pid_control_variable_saturated -
											  state->pid_control_variable,
											  pg_fixed32_from_double(0.5));
//		state->pid_integral += (state->pid_control_variable_saturated -
//								state->pid_control_variable) * Kaw;
	else
		state->pid_integral += (state->pid_control_variable_saturated -
								state->pid_control_variable) / -Kaw;
	i = state->pid_integral;

	/* Derivative component for change of error over time. */
	state->pid_derivative = error - state->pid_error;
	state->pid_error = error;
	if (Kd >= 0)
		d = pg_fixed32_mul(state->pid_derivative, pg_fixed32_from_double(0.3));
//		d = state->pid_derivative * Kd;
	else
		d = state->pid_derivative / -Kd;

	/* Control variable before clamping. */
	control_variable = p + i + d;

	/* Save clamp and non-clamped values for anti-windup. */
	state->pid_control_variable = control_variable;
	if (control_variable < min)
		control_variable = min;
	if (control_variable > max)
		control_variable = max;
	state->pid_control_variable_saturated = control_variable;
	
	elog(DEBUG1, "PID = (%.5f, %.1f, %.1f), %1.f - %.1f = %.1f, output: %.1f -> %.1f",
		 pg_fixed32_to_double(p),
		 pg_fixed32_to_double(i),
		 pg_fixed32_to_double(d),
		 pg_fixed32_to_double(setpoint),
		 pg_fixed32_to_double(process_variable),
		 pg_fixed32_to_double(error),
		 pg_fixed32_to_double(state->pid_control_variable),
		 pg_fixed32_to_double(state->pid_control_variable_saturated));

	return control_variable;
}

/*
 * Takes the number of IOs that are done according to pgaio_wref_check_done(),
 * for which pgaio_wait_one() has not yet been called.  Saturated should be
 * set to true when new IOs can't be started due to other limits (eg the
 * number of buffers needed would be too high), and max_ios should be set to
 * clamp the result.  Returns a new target IO concurrency level that tries to
 * drive ios_done to a level that avoids IO stalls, but not higher.
 */
static int16
aio_stream_controller_update(AioStreamController *state,
							 bool saturated,
							 int16 ios_target,
							 int16 ios_done,
							 int16 max_ios)
{
	pg_fixed32	done;
	pg_fixed32	error;
	pg_fixed32	min;
	pg_fixed32	max;
	pg_fixed32	control_variable;
	pg_fixed32	process_variable;
	pg_fixed32	setpoint;

	Assert(ios_done >= 0);
	Assert(ios_target > 0);

	/*
	 * Step #1: Find the minimum safe number of done IOs to maintain at all
	 * times to avoid stalls, considering the size of recent fluctuations.
	 *
	 * Assume that jitter produces normally distributed ios_done values over
	 * time.  Target a central limit that would make stalling a 4 sigma event.
	 * If that assumption is wrong, long tail events might produce stall, but
	 * that will be remembered for some time, and must be pretty hard to cope
	 * with anyway.  The only defense is likely massive oversupply of IO
	 * concurrency, so the working theory is that this simplifying assumption
	 * is a good tradeoff.
	 *
	 * XXX One source of fluctuations is batched submission, which might cause
	 * ios_done to vary by that amount, which might cause an overestimation,
	 * but that doesn't seem like a problem in itself?
	 *
	 * XXX This problem may be similar to "single item dynamic lot sizing"
	 * with "stochastic lead times" from operational research (keeping shop
	 * inventory or factory supplies at the right level efficiently).  In
	 * contrast with some of those algorithms (at a glance), this approach
	 * doesn't consider time or predict demand, or rather the time steps are
	 * in lockstep with controller iterations, so demand is 100% and precisely
	 * on time.  The stochastic element is when IOs are done, but they are
	 * counted in consumption time units.  Is there a way to avoid assuming
	 * normal distribution hiding in there?
	 */

	/* Update ios_done statistics. */
	done = pg_fixed32_from_int(ios_done);
	state->ios_done_ewma -= state->ios_done_ewma / AIO_SC_EWMA_ALPHA;
	state->ios_done_ewma += done / AIO_SC_EWMA_ALPHA;
	error = done - state->ios_done_ewma;
	state->ios_done_ewmv -= state->ios_done_ewmv / AIO_SC_EWMV_LAMBDA;
	state->ios_done_ewmv += pg_fixed32_square(error) / AIO_SC_EWMV_LAMBDA;

	/*
	 * XXX HACK
	 */

	/* First stage of bootstrapping the stream, before we have data. */
	if (ios_target < AIO_SC_MIN_IO_TARGET)
	{
		state->ios_done_target = pg_fixed32_from_int(AIO_SC_MIN_IO_DONE_TARGET);
		state->ios_done_ewmv_recompute_upper = 0;
		state->ios_done_ewmv_recompute_lower = 0;
		state->ios_done_ewma_recompute_upper = state->ios_done_target;
		state->ios_done_ewma_recompute_lower = state->ios_done_target;
		return Min(AIO_SC_MIN_IO_TARGET, max_ios);
	}

#if 0
	if (state->ios_done_ewma < pg_fixed32_from_int(AIO_SC_MIN_IO_DONE_TARGET))
	{
		pg_fixed32	target;
		pg_fixed32	stddev;

		/*
		 * Second stage of bootstrapping the stream.  We may never have to
		 * change from these values if the storage is fast enough without
		 * significant variance, eg well cached buffered IO.
		 */
		target = pg_fixed32_from_int(AIO_SC_MIN_IO_DONE_TARGET);
		stddev = pg_fixed32_mul(target, pg_fixed32_from_int(6827) / 10000 / 2);
		state->ios_done_target = target;
		state->ios_done_ewma_recompute_lower = state->ios_done_ewma - stddev;
		state->ios_done_ewma_recompute_upper = state->ios_done_ewma + stddev;
		state->ios_done_ewmv_recompute_lower = pg_fixed32_square(stddev / 2);
		state->ios_done_ewmv_recompute_upper = pg_fixed32_square(stddev * 2);
	}
#endif
	if (true ||
		state->ios_done_ewmv <= state->ios_done_ewmv_recompute_lower ||
		state->ios_done_ewmv >= state->ios_done_ewmv_recompute_upper ||
		state->ios_done_ewma <= state->ios_done_ewma_recompute_lower ||
		state->ios_done_ewma >= state->ios_done_ewma_recompute_upper)
	{
		double		variance;
		pg_fixed32	stddev;
		pg_fixed32	new_done_target;

		/*
		 * The variance has moved by one standard deviation's worth since last
		 * computation or the bootstrap value.  That's enough to change
		 * ios_done_target, so it's time time run the slower floating point
		 * calculation.
		 *
		 * The new target for ios_done is many standard deviations from the
		 * recent average, rounded up to a whole number.  If we drive
		 * ios_done_ewma to that level, then individual ios_done counts should
		 * be unlikely to drop to zero, assuming existing patterns persist,
		 * and we have a lot of time to adjust to a change, if it's at all
		 * possible.
		 *
		 * Record the thresholds at which we'll recompute the target.
		 */
		variance = pg_fixed32_to_double(state->ios_done_ewmv);
		stddev = pg_fixed32_from_double(sqrt(variance));
		new_done_target = pg_fixed32_ceil(stddev * 4);

		/* Second stage of boostrapping: ramp up one by one to desired level. */
		if (state->ios_done_target < new_done_target - 1)
			state->ios_done_target += pg_fixed32_from_int(1);
		else
			state->ios_done_target = new_done_target;

		state->ios_done_ewmv_recompute_lower = pg_fixed32_square(stddev / 2);
		state->ios_done_ewmv_recompute_upper = pg_fixed32_square(stddev * 2);
		state->ios_done_ewma_recompute_lower = state->ios_done_ewma - stddev;
		state->ios_done_ewma_recompute_upper = state->ios_done_ewma + stddev;

		elog(DEBUG1, "aio_stream_controller_update: stddev = %.1f",
			 pg_fixed32_to_double(stddev));
	}

	/*
	 * Step 2: Find ios_target that brings ios_done_ewma to ios_done_target.
	 *
	 * Turning ios_target up doesn't always make ios_done_target go up,
	 * because IOs may be queueing up somewhere instead of running
	 * concurrently.  That problem is handled by step 3, below.
	 *
	 * XXX put that back
	 *
	 * Even when it does, its effects are delayed and prone to jitter and
	 * shocks, and also competes for resources with other users, so a simple
	 * "proportional" control is likely to oscillate, leading to stalls.
	 * Therefore, use a PID controller to drive ios_done_ewma towards
	 * ios_done_target.
	 */

	/* Provide current and target values for ios_done. */
	process_variable = done;
	setpoint = state->ios_done_target;

	/* Provide clamp range for this iteration. */
#if 0
	min = pg_fixed32_from_int(Max(AIO_SC_MIN_IO_DONE_TARGET, ios_target / 2));
	if (saturated)
		max = pg_fixed32_from_int(Min(ios_target + 1, max_ios));
	else
		max = pg_fixed32_from_int(Min(ios_target * 2, max_ios));
#endif
	min = pg_fixed32_from_int(Max(AIO_SC_MIN_IO_DONE_TARGET, ios_target + 1));
	max = pg_fixed32_from_int(Min(ios_target + 1, max_ios));

	/* Compute new value for ios_target. */
	control_variable = aio_stream_controller_pid(state,
												 AIO_SC_PID_KP,
												 AIO_SC_PID_KI,
												 AIO_SC_PID_KD,
												 AIO_SC_PID_KAW,
												 min,
												 max,
												 setpoint,
												 process_variable);

#if 1
	{
		int fd = open("/tmp/plotme.data", O_CREAT | O_RDWR | O_APPEND, 0666);
		char line[80];
		snprintf(line,
				 sizeof(line), "%.3f,%.3f,%.3f,%d\n",
				 pg_fixed32_to_double(state->ios_done_target),
				 pg_fixed32_to_double(done),
				 pg_fixed32_to_double(state->ios_done_ewma),
				 pg_fixed32_round_int(control_variable));
		write(fd, line, strlen(line));
		close(fd);
	}
#endif
#if 1
	elog(DEBUG1,
		 "ios_done = %d (target=%.3f, avg=%.3f [%.3f..%.3f], var=%.3f [%.3f..%.3f]), ios_target %d -> %d",
		 ios_done,
		 pg_fixed32_to_double(state->ios_done_target),
		 pg_fixed32_to_double(state->ios_done_ewma),
		 pg_fixed32_to_double(state->ios_done_ewma_recompute_lower),
		 pg_fixed32_to_double(state->ios_done_ewma_recompute_upper),
		 pg_fixed32_to_double(state->ios_done_ewmv),
		 pg_fixed32_to_double(state->ios_done_ewmv_recompute_lower),
		 pg_fixed32_to_double(state->ios_done_ewmv_recompute_upper),
		 ios_target,
		 pg_fixed32_round_int(control_variable));
#endif

	return pg_fixed32_round_int(control_variable);
}

#endif
