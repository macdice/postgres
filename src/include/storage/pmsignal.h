/*-------------------------------------------------------------------------
 *
 * pmsignal.h
 *	  routines for signaling between the postmaster and its child processes
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pmsignal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PMSIGNAL_H
#define PMSIGNAL_H

#include <signal.h>

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#ifdef HAVE_SYS_PROCCTL_H
#include <sys/procctl.h>
#endif

/*
 * Reasons for signaling the postmaster.  We can cope with simultaneous
 * signals for different reasons.  If the same reason is signaled multiple
 * times in quick succession, however, the postmaster is likely to observe
 * only one notification of it.  This is okay for the present uses.
 */
typedef enum
{
	PMSIGNAL_RECOVERY_STARTED,	/* recovery has started */
	PMSIGNAL_RECOVERY_CONSISTENT,	/* recovery has reached consistent state */
	PMSIGNAL_BEGIN_HOT_STANDBY, /* begin Hot Standby */
	PMSIGNAL_ROTATE_LOGFILE,	/* send SIGUSR1 to syslogger to rotate logfile */
	PMSIGNAL_START_AUTOVAC_LAUNCHER,	/* start an autovacuum launcher */
	PMSIGNAL_START_AUTOVAC_WORKER,	/* start an autovacuum worker */
	PMSIGNAL_BACKGROUND_WORKER_CHANGE,	/* background worker state change */
	PMSIGNAL_START_WALRECEIVER, /* start a walreceiver */
	PMSIGNAL_ADVANCE_STATE_MACHINE, /* advance postmaster's state machine */
	PMSIGNAL_XLOG_IS_SHUTDOWN,	/* ShutdownXLOG() completed */
} PMSignalReason;

#define NUM_PMSIGNALS (PMSIGNAL_XLOG_IS_SHUTDOWN+1)

/*
 * Reasons why the postmaster would send SIGQUIT to its children.
 */
typedef enum
{
	PMQUIT_NOT_SENT = 0,		/* postmaster hasn't sent SIGQUIT */
	PMQUIT_FOR_CRASH,			/* some other backend bought the farm */
	PMQUIT_FOR_STOP,			/* immediate stop was commanded */
} QuitSignalReason;

/* PMSignalData is an opaque struct, details known only within pmsignal.c */
typedef struct PMSignalData PMSignalData;

#ifdef EXEC_BACKEND
extern PGDLLIMPORT volatile PMSignalData *PMSignalState;
#endif

/*
 * prototypes for functions in pmsignal.c
 */
extern Size PMSignalShmemSize(void);
extern void PMSignalShmemInit(void);
extern void SendPostmasterSignal(PMSignalReason reason);
extern bool CheckPostmasterSignal(PMSignalReason reason);
extern void SetQuitSignalReason(QuitSignalReason reason);
extern QuitSignalReason GetQuitSignalReason(void);
extern void MarkPostmasterChildSlotAssigned(int slot);
extern bool MarkPostmasterChildSlotUnassigned(int slot);
extern bool IsPostmasterChildWalSender(int slot);
extern void RegisterPostmasterChildActive(void);
extern void MarkPostmasterChildWalSender(void);
extern bool PostmasterIsAlive(void);
extern void PostmasterDeathSignalInit(void);
extern void ExitOnPostmasterDeath(void);

#endif							/* PMSIGNAL_H */
