#ifndef AIO_STREAM_CONTROLLER_H
#define AIO_STREAM_CONTROLLER_H

#include <math.h>
#include <fcntl.h>

#include "lib/pg_fixedpoint.h"

/* Starting levels. */
#define AIO_SC_MIN_IO_TARGET 2
#define AIO_SC_MIN_IO_DONE_TARGET 2

/* Smoothing constants for ios_done. */
#define AIO_SC_EWMA_ALPHA 32

/* Gain constants used for PID controller. */
#define AIO_SC_PID_KP pg_fixed32_from_int(20) / 100
#define AIO_SC_PID_KI pg_fixed32_from_int(20) / 100
#define AIO_SC_PID_KD pg_fixed32_from_int(30) / 100
#define AIO_SC_PID_KAW pg_fixed32_from_int(50) / 100

typedef struct AioStreamController
{
	pg_fixed32	ios_done_ewma;
	pg_fixed32	ios_done_target;

	pg_fixed32	pid_integral;
	pg_fixed32	pid_derivative;
	pg_fixed32	pid_control_variable;
	pg_fixed32	pid_control_variable_saturated;
	pg_fixed32	pid_error;
} AioStreamController;

/*
 * https://en.wikipedia.org/wiki/Proportional-integral-derivative_controller
 */
static inline pg_fixed32
aio_stream_controller_pid(AioStreamController *state,
						  pg_fixed32 Kp,
						  pg_fixed32 Ki,
						  pg_fixed32 Kd,
						  pg_fixed32 Kaw,
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
	p = pg_fixed32_mul(error, Kp);

	/* Integral component for accumulating error with anti-windup. */
	state->pid_integral += pg_fixed32_mul(error, Ki);
	state->pid_integral += pg_fixed32_mul(state->pid_control_variable_saturated -
										  state->pid_control_variable,
										  Kaw);
	i = state->pid_integral;

	/* Derivative component for change of error over time. */
	d = pg_fixed32_mul(error - state->pid_error, Kd);
	state->pid_error = error;

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
 * Compute new target number IOs to drive ios_done to
 * AIO_SC_MIN_IO_DONE_TARGET.
 */
static int16
aio_stream_controller_update(AioStreamController *state,
							 int16 ios_in_progress,
							 int16 ios_done,
							 int16 max_ios)
{
	pg_fixed32	min;
	pg_fixed32	max;
	pg_fixed32	control_variable;
	pg_fixed32	process_variable;
	pg_fixed32	setpoint;

	Assert(ios_in_progress > 0);

	/* Smoothed version of ios_done. */
	state->ios_done_ewma -= state->ios_done_ewma / AIO_SC_EWMA_ALPHA;
	state->ios_done_ewma += pg_fixed32_from_int(ios_done) / AIO_SC_EWMA_ALPHA;

	/* Bootstrap. */
	if (state->ios_done_target == 0)
		state->ios_done_target = AIO_SC_MIN_IO_DONE_TARGET;
	if (ios_in_progress < AIO_SC_MIN_IO_TARGET)
		return Min(ios_in_progress + 1, max_ios);

	/* XXX Extremely dumb version: if it drops to zero, aim higher */
	if (ios_done == 0)
		state->ios_done_target += pg_fixed32_from_int(1);

	/* Compute new value for ios_target. */
	min = pg_fixed32_from_int(Max(AIO_SC_MIN_IO_DONE_TARGET, ios_in_progress - 1));
	max = pg_fixed32_from_int(Min(ios_in_progress + 1, max_ios));
	process_variable = state->ios_done_ewma;
	setpoint = state->ios_done_target;
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
				 sizeof(line), "%.3f,%d,%.3f,%d\n",
				 pg_fixed32_to_double(state->ios_done_target),
				 ios_done,
				 pg_fixed32_to_double(state->ios_done_ewma),
				 pg_fixed32_round_int(control_variable));
		write(fd, line, strlen(line));
		close(fd);
	}
#endif

	return pg_fixed32_round_int(control_variable);
}

#endif
