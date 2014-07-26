/*-------------------------------------------------------------------------
 *
 * lockwaitpolicy.h
 *	  Header file for the enum LockWaitPolicy enum, which is needed in
 *    several modules (parser, planner, executor, heap manager).
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
 */
typedef enum
{
	/* Wait for the lock to become available */
	LockWaitBlock,
	/* SELECT FOR UPDATE SKIP LOCKED, skipping rows that can't be locked */
	LockWaitSkip,
	/* SELECT FOR UPDATE NOWAIT, abandoning the transaction */
	LockWaitError
} LockWaitPolicy;

#endif   /* LOCKWAITPOLICY_H */
