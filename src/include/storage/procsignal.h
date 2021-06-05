/*-------------------------------------------------------------------------
 *
 * procsignal.h
 *	  Routines for interprocess signaling
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procsignal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCSIGNAL_H
#define PROCSIGNAL_H

#include "storage/backendid.h"


typedef enum
{
	/*
	 * XXX. PROCSIGNAL_BARRIER_PLACEHOLDER should be replaced when the first
	 * real user of the ProcSignalBarrier mechanism is added. It's just here
	 * for now because we can't have an empty enum.
	 */
	PROCSIGNAL_BARRIER_PLACEHOLDER = 0
} ProcSignalBarrierType;

/*
 * prototypes for functions in procsignal.c
 */
extern Size ProcSignalShmemSize(void);
extern void ProcSignalShmemInit(void);

extern void ProcSignalInit(int pss_idx);

extern uint64 EmitProcSignalBarrier(ProcSignalBarrierType type);
extern void WaitForProcSignalBarrier(uint64 generation);
extern void ProcessProcSignalBarrier(void);

#endif							/* PROCSIGNAL_H */
