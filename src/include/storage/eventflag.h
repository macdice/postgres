/*
 * A simple IPC mechanism that allows at most one backend to wait for and
 * process an event.
 */
#ifndef EVENTFLAG_H
#define EVENTFLAG_H

#include "port/atomics.h"

typedef struct EventFlag
{
	pg_atomic_uint32 word;
} EventFlag;

extern void eventflag_init(EventFlag *flag);
extern bool eventflag_wait_exclusive(EventFlag *flag, uint32 wait_event_info);
extern bool eventflag_begin_wait_exclusive(EventFlag *flag);
extern void eventflag_fire(EventFlag *flag);
extern bool eventflag_has_fired(EventFlag *flag);

#endif
