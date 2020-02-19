/*-------------------------------------------------------------------------
 *
 * htm.c
 *	  Code to decide whether HTM is available on this micro-architecture.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/port/htm.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <setjmp.h>
#include <signal.h>

#include "port/htm.h"

#ifdef HAVE_HTM

/* Global variable to advertise whether HTM is available. */
bool have_htm_support;

static sigjmp_buf illegal_instruction_jump;

static void
illegal_instruction_handler(SIGNAL_ARGS)
{
	siglongjmp(illegal_instruction_jump, 1);
}

static bool
test_memory_transaction(void)
{
	/*
	 * We don't really care if this transaction commits or aborts, we just want
	 * to exercise the instructions and trigger a SIGILL if they aren't there.
	 */
	if (!pg_htm_begin())
		return false;
	pg_htm_commit();
	return true;
}

/*
 * Test whether we have HTM support in every backend process.
 */
void
htm_init(void)
{
	/*
	 * You could use the Intel CPUID feature test for this, but perhaps a
	 * SIGILL-based approach will eventually work on other ISAs that grow HTM
	 * support.
	 *
	 * TODO: Is this going to work on Windows/MSVC?
	 */
	pqsignal(SIGILL, illegal_instruction_handler);
	if (sigsetjmp(illegal_instruction_jump, 1) == 0)
	{
		/* Try to use HTM instructions */
		test_memory_transaction();
		have_htm_support = true;
	}
	else
	{
		/* We got the SIGILL trap */
		have_htm_support = false;
	}
	pqsignal(SIGILL, SIG_DFL);
}

#endif
