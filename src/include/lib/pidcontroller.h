#ifndef PIDCONTROLLER_H

typedef struct PIDController
{
	double		p;				/* proportional gain */
	double		i;				/* integral gain */
	double		d;				/* derivative gain */
	double		anti_windup;	/* anti-windup gain */
	double		time_constant;	/* for derivative filtering */
	double		time_step;

	double		integral;
	double		old_error;
	double		old_derivative;
	double		old_control;
	double		old_saturated_control;
}			PIDController;

static inline void
PIDControllerInit(PIDController * pid,
				  double p, double i, double d, double anti_windup,
				  double time_constant, double time_step)
{
	pid->p = p;
	pid->i = i;
	pid->d = d;
	pid->anti_windup = anti_windup;
	pid->time_constant = time_constant;
	pid->time_step = time_step;
	pid->integral = 0;
	pid->old_error = 0;
	pid->old_derivative = 0;
	pid->old_control = 0;
	pid->old_saturated_control = 0;
}

/*
 * XXX control clamp values passed as arguments so that we can restrict the
 * value to the range 0.5..2.0 of the old value.  better way?
 */
static inline double
PIDControllerUpdate(PIDController * pid,
					double setpoint,
					double process,
					double min_control,
					double max_control)
{
	double		derivative_filtered;
	double		saturated_control;
	double		control;
	double		error;

	error = setpoint - process;
	pid->integral += pid->i * error * pid->time_step;
	pid->integral += pid->anti_windup *
		(pid->old_saturated_control - pid->old_control) *
		pid->time_step;
	derivative_filtered =
		(error - pid->old_error + pid->time_constant * pid->old_derivative) /
		(pid->time_step + pid->time_constant);
	control = pid->p * error + pid->integral + pid->d * derivative_filtered;
	if (control > max_control)
		saturated_control = max_control;
	else if (control < min_control)
		saturated_control = min_control;
	else
		saturated_control = control;

	pid->old_error = error;
	pid->old_control = control;
	pid->old_derivative = derivative_filtered;
	pid->old_saturated_control = saturated_control;

	return saturated_control;
}

#endif
