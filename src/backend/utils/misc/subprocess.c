#include "postgres.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/subprocess.h"
#include "utils/wait_event.h"

#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * Start a subprocess using a shell command.  Return a PID on success, -1 on
 * failure, with errno set by a system call.  When followed by a
 * wait_latch_or_subprocess() loop, this is a replacement for standard
 * system().  See wait_latch_or_subprocess() loop for more.
 */
pid_t
start_shell_subprocess(Subprocess *sp, const char *shell_command)
{
	sigset_t newsigblock;
	sigset_t oldsigblock;

	memset(sp, 0, sizeof(*sp));

	/*
	 * We need to be able to wait for the child to exit, but we also want to
	 * multiplex that with other events.  We could install a temporary SIGCHLD
	 * handler for that, but that might get tricky if the caller was also
	 * expecting SIGCHLD for something.  To do this, we'll create a special
	 * pipe that has no other purpose.
	 */
	ReserveExternalFD();
	ReserveExternalFD();
	if (pipe(sp->canary_pipe) < 0)
	{
		ReleaseExternalFD();
		ReleaseExternalFD();
		return -1;
	}

	/* Block all signals, to give the child time to set signal dispositions. */
	sigfillset(&newsigblock);
	sigprocmask(SIG_BLOCK, &newsigblock, &oldsigblock);

	sp->pid = fork();
	if (sp->pid == -1)
	{
		/* Failed to fork. */
		sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
		close(sp->canary_pipe[0]);
		close(sp->canary_pipe[1]);
		ReleaseExternalFD();
		ReleaseExternalFD();
		return -1;
	}
	else if (sp->pid == 0)
	{
		/* In child, close the read end of pipe. */
		close(sp->canary_pipe[0]);

		/*
		 * XXX It would be a good idea to close all the other descriptors that
		 * the backend hold, and allow only the canary to make it to the other
		 * side of execl().  Rather than trying to find them all from here, we
		 * should probably investigate O_CLOEXEC, FD_CLOEXEC and (non-standard)
		 * SOCK_CLOEXEC flags on all descriptors.
		 */

		/* Revert to default signal actions and then unblock. */
		pqsignal(SIGHUP, SIG_DFL);
		pqsignal(SIGINT, SIG_DFL);
		pqsignal(SIGQUIT, SIG_DFL);
		pqsignal(SIGTERM, SIG_DFL);
		pqsignal(SIGALRM, SIG_DFL);
		pqsignal(SIGPIPE, SIG_DFL);
		pqsignal(SIGUSR1, SIG_DFL);
		pqsignal(SIGUSR2, SIG_DFL);
		sigprocmask(SIG_SETMASK, &oldsigblock, NULL);

		/* Become the shell command. */
		execl("/bin/sh", "sh", "-c", shell_command, NULL);
		_exit(127);	/* not reached unless the above fails */
	}

	/* In parent, close the write end of pipe. */
	close(sp->canary_pipe[1]);
	ReleaseExternalFD();
	sigprocmask(SIG_SETMASK, &oldsigblock, NULL);

	return sp->pid;
}

/*
 * Wait for a subprocess to finished, and collect its exit status.  While
 * waiting, also exit if the postmaster dies, and return control to the caller
 * temporarily if the latch is set.
 *
 * Return 0 when the subprocess is finished and its return status has been
 * written to *wstatus.
 *
 * Return WL_LATCH_SET to indicate that the latch has been set.  The caller
 * should reset it and handle any requests, but should not use
 * CHECK_FOR_INTERRUPTS(), ereport() or other forms of non-local exit until
 * wait_subprocess() eventually returns 0 to indicate that the subprocess has
 * finished, to avoid leaking zombie processes.  The caller is free to call
 * proc_exit() if it would like to shut down quickly in response to a latch; in
 * that case, the subprocess will continue running as a re-parented orphan, and
 * be eventually be reaped by init.
 */
int
wait_latch_or_subprocess(Latch *latch, Subprocess *sp, int *wstatus, int wait_event)
{
	pid_t pid;
	int rc;

	rc = WaitLatchOrSocket(latch,
						   WL_LATCH_SET | WL_SOCKET_READABLE | WL_POSTMASTER_DEATH,
						   sp->canary_pipe[0], -1, wait_event);

	if (rc == WL_SOCKET_READABLE)
	{
		do
		{
			pgstat_report_wait_start(wait_event);
			pid = waitpid(sp->pid, wstatus, 0);
			pgstat_report_wait_end();
		} while (pid == -1 && errno == EINTR);

		if (pid == -1)
			elog(ERROR, "%s failed: %m", "waitpid");

		Assert(pid == sp->pid);

		/* Close read end of pipe. */
		close(sp->canary_pipe[0]);
		ReleaseExternalFD();

		return 0;
	}

	if (rc == WL_POSTMASTER_DEATH)
	{
		/*
		 * The postmaster has gone away.  Try to tell the subprocess to quit
		 * (though what that program will do with SIGQUIT is not known to us),
		 * and then exit immediately as we do elsewhere for postmaster death.
		 */
		kill(sp->pid, SIGQUIT);
		proc_exit(1);
	}

	Assert(rc == WL_LATCH_SET);

	return rc;
}
