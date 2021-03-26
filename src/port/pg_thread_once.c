#include "postgres.h"

#include "port/pg_thread_mutex.h"
#include "port/pg_thread_once.h"

int
pg_thread_once(pg_thread_once_t *once_control,
			   void (*init_routine)(void))
{
#if defined(WIN32)
	if (!once_control->initialized)
	{
		pg_thread_mutex_lock(&once_control->mutex);
		if (!once_control->initialized)
		{
			init_routine();
			once_control->initialized = true;
		}
		pg_thread_mutex_unlock(&once_control->mutex);
	}
#else
	return pthread_once(once_control, init_routine);
#endif
}
