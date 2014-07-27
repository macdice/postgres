/*-------------------------------------------------------------------------
 *
 * lockwaitpolicy.h
 *	  Header file for the enum LockWaitPolicy enum, which is needed in
 *	  several modules (parser, planner, executor, heap manager).
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 * src/include/utils/lockwaitpolicy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKWAITPOLICY_H
#define LOCKWAITPOLICY_H

/*
 * Policy for what to do when a row lock cannot be obtained immediately.
 * Order is important -- see applyLockingClause.
 */
typedef enum
{
	/* Wait for the lock to become available */
	LockWaitBlock = 1,
	/* SELECT FOR UPDATE SKIP LOCKED, skipping rows that can't be locked */
	LockWaitSkip = 2,
	/* SELECT FOR UPDATE NOWAIT, abandoning the transaction */
	LockWaitError = 3
} LockWaitPolicy;

#endif   /* LOCKWAITPOLICY_H */
