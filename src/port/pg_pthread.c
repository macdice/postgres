#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "port/pg_pthread.h"

__declspec(thread) pthread_t pthread_win32_self;

