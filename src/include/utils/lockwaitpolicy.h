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
 *
 * The enum values defined here control how the parser treats multiple FOR
 * UPDATE/SHARE clauses that affect the same table.  If multiple locking
 * clauses are defined then the one with the highest numerical value takes
 * precedence -- see applyLockingClause.
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
