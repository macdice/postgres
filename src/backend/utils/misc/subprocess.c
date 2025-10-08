/*
 * Replacement for system(), popen() and OpenPipeStream() that supports
 * CHECK_FOR_INTERRUPTS().
 *
 * Synchronous read/write operations are provided as drop-in replacements for
 * fread()/fwrite().  In future work, completion-based interfaces could be
 * provided for more efficient asynchronous I/O.  On Windows, asynchronous I/O
 * is used internally, as required to multiplex pipe I/O with latches, but
 * that is not exposed to callers yet.
 */

#include "postgres.h"

#include "lib/ilist.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "utils/subprocess.h"
#include "utils/wait_event.h"

#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef WIN32
#include <spawn.h>
#endif

#ifndef WIN32
/*
 * To use SIGCHLD and waitpid(), we need a table pid->Subprocess.
 *
 * XXX If waiteventset.c supported WL_SUBPROCESS with io_uring/kqueue, we
 * wouldn't need SIGCHLD or the subprocess table.
 */
#define HAVE_SUBPROCESS_TABLE
#endif

typedef enum SubprocessStatus
{
	SUBPROCESS_STATUS_RUNNING,
	SUBPROCESS_STATUS_ABANDONED,	/* closed but not yet reaped */
	SUBPROCESS_STATUS_REAPED	/* reaped but not yet closed */
}			SubprocessStatus;

struct Subprocess
{
	pid_t		pid;
	ProcNumber	owner_procno;
	int			flags;
	int			exit_status;
	SubprocessStatus status;
	ResourceOwner resowner;

#ifndef WIN32
	int			pipe_fd;
#else
	CRITICAL_SECTION status_lock;
	HANDLE		job_handle;
	HANDLE		process_handle;
	HANDLE		pipe_handle;
	OVERLAPPED	io_in_progress;
#endif

#ifdef HAVE_SUBPROCESS_TABLE
	dlist_node	subprocess_table_node;
#endif

	/*
	 * To support an fread()/fwrite()-style buffered synchronous interface, we
	 * need our own internal buffer to consolidate system calls.
	 */
	size_t		buffer_index;
	size_t		buffer_size;
	PGIOAlignedBlock buffer[FLEXIBLE_ARRAY_MEMBER];
};

#ifdef HAVE_SUBPROCESS_TABLE
static dlist_head subprocess_table = DLIST_STATIC_INIT(subprocess_table);
#endif

static void ResOwnerReleaseSubprocess(Datum res);
static char *ResOwnerPrintSubprocess(Datum res);

static const ResourceOwnerDesc subprocess_resowner_desc =
{
	.name = "Subprocess",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_SUBPROCESSES,
	.ReleaseResource = ResOwnerReleaseSubprocess,
	.DebugPrint = ResOwnerPrintSubprocess
};

volatile sig_atomic_t SubprocessExitPending;

static void
ResOwnerReleaseSubprocess(Datum res)
{
	Subprocess *sp = (Subprocess *) DatumGetPointer(res);

	/*
	 * Suppress explicit resource owner management, since cleanup is in
	 * progress and will handle that automatically.
	 */
	sp->resowner = NULL;

	CloseSubprocess(sp);
}

static char *
ResOwnerPrintSubprocess(Datum res)
{
	Subprocess *sp = (Subprocess *) DatumGetPointer(res);

	return psprintf("Subprocess pid %d", sp->pid);
}

static void
LockSubprocessTable(void)
{
#ifdef HAVE_SUBPROCESS_TABLE
	/*
	 * Currently no locking is required, because each process has its own
	 * subprocess table and only accesses it from CHECK_FOR_INTERRUPTS().
	 *
	 * XXX:MT These functions mark out future concurrency concerns: other
	 * backend threads might reap processes after handling SIGCHLD, concurrent
	 * OpenSubprocess() calls could leak each other's pipes, and concurrent
	 * reaping and forking of a recycled pid could corrupt our table.  (Better
	 * to get rid of SIGCHLD, see above.)
	 */
#endif
}

static void
UnlockSubprocessTable(void)
{
#ifdef HAVE_SUBPROCESS_TABLE
	/* XXX:MT see above */
#endif
}

static void
LockSubprocess(Subprocess *sp)
{
#ifdef WIN32
	/*
	 * Currently we need a Windows native lock per subprocess, to serialize
	 * the main thread against the process exit callback that runs in a system
	 * thread.
	 *
	 * XXX That problem could be removed by supporting WL_SUBPROCESS in
	 * waitevent.c backed by IOCP, moving all processing to the main thread.
	 */
	EnterCriticalSection(&sp->status_lock);
#endif
}

static void
UnlockSubprocess(Subprocess *sp)
{
#ifdef WIN32
	LeaveCriticalSection(&sp->status_lock);
#endif
}

#ifdef HAVE_SUBPROCESS_TABLE
static void
SignalHandlerForSubprocessExit(SIGNAL_ARGS)
{
	fprintf(stderr, "SignalHandlerForSubprocessExit\n");
	SubprocessExitPending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

/*
 * Just like system() and popen(), install a SIGCHLD handler but only while
 * any subprocess is running.
 *
 */
static void
AdjustSubprocessExitHandler(bool adding)
{
	/*
	 * XXX:MT This would need to be the thread signal mask.  It's probably
	 * better to concentrate SIGCHLD signals into the few backend threads that
	 * are likely to be interested, hence disabling the handler when it's not
	 * useful.
	 */
	if (dlist_is_empty(&subprocess_table))
	{
		if (adding)
			pqsignal(SIGCHLD, SignalHandlerForSubprocessExit);
		else
			pqsignal(SIGCHLD, SIG_DFL);
	}
}
#endif

/*
 * Remove a subprocess from the subprocess table, if there is one on this
 * platform.
 */
static void
ForgetSubprocess(Subprocess *sp)
{
#ifdef HAVE_SUBPROCESS_TABLE
	dlist_delete(&sp->subprocess_table_node);
	AdjustSubprocessExitHandler(false);
#endif
}

/*
 * Wait for a subprocess to exit, and return its exit status.  Interrupts must
 * not be held.
 */
int
WaitSubprocess(Subprocess *sp)
{
	int			exit_status;

	Assert(InterruptHoldoffCount == 0);

	/* Flush any buffered data first. */
	if (sp->flags & SUBPROCESS_WRITE)
		WriteSubprocess(sp, NULL, 0);

	while (!GetSubprocessExitStatus(sp, &exit_status))
	{
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1,
				  WAIT_EVENT_SUBPROCESS_EXIT);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	return exit_status;
}

/*
 * Run a subprocess using a shell command, in the style of system().  Unlike
 * system(), interrupts are processed, and the process receives SIGTERM if
 * cancelled by an interrupt before the subprocess is reaped.  The environment
 * may also be passed explicitly: see OpenSubprocess() for details.
 *
 * Returns the exit status of the shell.  An exit status of 127 means that the
 * execution of the shell failed, like system().
 */
int
RunSubprocess(const char *shell_command, char *const envp[])
{
	Subprocess *sp;
	int			exit_status;

	sp = OpenSubprocess(shell_command, 0, envp);
	if (sp == NULL)
		return 127;
	exit_status = WaitSubprocess(sp);
	CloseSubprocess(sp);

	return exit_status;
}

/*
 * Record exit status.  Subprocess table must be locked if needed on this
 * platform.
 */
static void
RecordSubprocessExitStatus(Subprocess *sp, int exit_status)
{
	bool		unreferenced = false;

	LockSubprocess(sp);			/* Windows only */
	if (sp->status == SUBPROCESS_STATUS_RUNNING)
	{
		/* Backend interested in exit status. */
		sp->exit_status = exit_status;
		sp->status = SUBPROCESS_STATUS_REAPED;
		SetLatch(&GetPGProcByNumber(sp->owner_procno)->procLatch);
	}
	else if (sp->status == SUBPROCESS_STATUS_ABANDONED)
	{
		/* Backend no longer interested in exit status. */
		ForgetSubprocess(sp);
		unreferenced = true;
	}
	else
	{
		Assert(false);
	}
	UnlockSubprocess(sp);

	if (unreferenced)
		free(sp);
}

/*
 * Reap as many child process exit statuses as possible without waiting.  This
 * is called by CHECK_FOR_INTERRUPTS(), after SIGCHLD has been received.
 */
void
ProcessSubprocessExit(void)
{
#ifndef WIN32
	pid_t		pid;
	int			exit_status;

	elog(LOG, "ProcessSubprocessExit");

	/*
	 * XXX:MT Serialize against concurrent changes to the subprocess table in
	 * other threads.
	 */
	LockSubprocessTable();
	while ((pid = waitpid(-1, &exit_status, WNOHANG)) > 0)
	{
		bool		found = false;
		dlist_iter	iter;

		dlist_foreach(iter, &subprocess_table)
		{
			Subprocess *sp = dlist_container(Subprocess,
											 subprocess_table_node,
											 iter.cur);

			if (sp->pid == pid)
			{
				RecordSubprocessExitStatus(sp, exit_status);
				found = true;
				break;
			}
		}

		/*
		 * Unknown pids imply that forking is happening outside this module,
		 * which is not allowed.
		 */
		if (!found)
			elog(PANIC, "reaped pid %d but it is not a known subprocess", pid);
	}
	UnlockSubprocessTable();
#endif
}

/*
 * Start a subprocess using a shell command.  Like standard system() and
 * popen(), the shell command is run with /bin/sh.
 *
 * If flags is SUBPROCESS_WRITE or SUBPROCESS_READ, data may be streamed to or
 * from the subprocess.  Bidirectional pipes are not currently supported due to
 * the risk of buffer deadlock with the existing synchronous interfaces.
 *
 * If flags is 0, an asynchronous equivalent of system() is started.  See also
 * RunSubprocess() which handles opening, waiting and closing in one step.
 *
 * Environment variables may be passed explicitly as a NULL-terminated array
 * of pointers to "NAME=VALUE" strings.  A NULL pointer selects the current
 * environment without changes.  Since setenv() is generally not allowed in
 * PostgreSQL code, new environment variables can be provided to the
 * subprocess by copying the current environment (environ) and making any
 * changes required.
 *
 * The subprocess is associated with the current resource owner.  Before the
 * resource owner goes out of scope, CloseSubprocess() should normally be
 * called.  If an error is raised, it will be closed automatically during
 * cleanup.
 *
 * Returns NULL and sets errno if an error occurs while starting the shell.
 */
Subprocess *
OpenSubprocess(const char *shell_command, int flags, char *const envp[])
{
	Subprocess *sp;

	if ((flags & SUBPROCESS_READ) && (flags & SUBPROCESS_WRITE))
		elog(ERROR, "subprocesses with bidirectional pipes not supported");
	if (flags & ~(SUBPROCESS_READ | SUBPROCESS_WRITE))
		elog(ERROR, "unknown subprocess flag");

	ResourceOwnerEnlarge(CurrentResourceOwner);

	/* We need memory that least as long as CurrentResourceOwner. */
	sp = malloc(offsetof(Subprocess, buffer) +
				flags ? SUBPROCESS_BUFFER_SIZE : 0);
	if (sp == NULL)
		return NULL;

	sp->owner_procno = MyProcNumber;
	sp->flags = flags;
	sp->status = SUBPROCESS_STATUS_RUNNING;
	sp->buffer_index = 0;
	sp->buffer_size = 0;
#ifdef WIN32
	InitializeCriticalSection(&sp->status_lock);
#endif

#ifndef WIN32
	LockSubprocessTable();
	do
	{
		char	   *argv[] = {"sh", "-c", unconstify(char *, shell_command), NULL};
		char	   *path = "/bin/sh";
		int			external_fds = 0;
		bool		have_file_actions = false;
		bool		have_attr = false;
		bool		have_pipe = false;
		posix_spawn_file_actions_t file_actions;
		posix_spawnattr_t attr;
		sigset_t	mask;
		int			pipe_fds[2];
		int			save_errno;

		if ((errno = posix_spawn_file_actions_init(&file_actions)) != 0)
			goto fail;
		have_file_actions = true;
		if ((errno = posix_spawnattr_init(&attr)) != 0)
			goto fail;
		have_attr = true;

		/* All signals set to default action in child. */
		sigfillset(&mask);
		if ((errno = posix_spawnattr_setsigdefault(&attr, &mask)) != 0)
			goto fail;

		/* Optional pipe connected to stdin/stdout in child. */
		if (flags)
		{
			int			close_fd;
			int			dup2_source_fd;
			int			dup2_target_fd;

			/* Make pipe. */
			if (!AcquireExternalFD())
				goto fail;
			external_fds++;
			if (!AcquireExternalFD())
				goto fail;
			external_fds++;
			if (pipe(pipe_fds) < 0)
				goto fail;
			have_pipe = true;

			/*
			 * Make the parent's end of the pipe non-blocking.
			 *
			 * XXX We could use POSIX 2024 pipe2() to skip two system calls
			 * here, where available.
			 */
			if (!pg_set_noblock(pipe_fds[flags & SUBPROCESS_READ ? 1 : 0]))
				goto fail;

			/* Tell child what to do with pipe ends. */
			if (flags & SUBPROCESS_READ)
			{
				close_fd = pipe_fds[0];
				dup2_source_fd = pipe_fds[1];
				dup2_target_fd = STDOUT_FILENO;
			}
			else
			{
				close_fd = pipe_fds[1];
				dup2_source_fd = pipe_fds[0];
				dup2_target_fd = STDIN_FILENO;
			}
			if ((errno = posix_spawn_file_actions_addclose(&file_actions,
														   close_fd)) != 0)
				goto fail;
			if ((errno = posix_spawn_file_actions_adddup2(&file_actions,
														  dup2_source_fd,
														  dup2_target_fd)) != 0)
				goto fail;
			if ((errno = posix_spawn_file_actions_addclose(&file_actions,
														   dup2_source_fd)) != 0)
				goto fail;
		}

		/* Make sure the SIGCHLD handler is installed before spawning. */
		AdjustSubprocessExitHandler(true);

		/*
		 * Spawn the process.  The main reason for using posix_spawn over
		 * traditional fork()/exec() is that it has access to vfork(), clone()
		 * or rfork(), which can skip overheads.  We could just use vfork()
		 * ourselves, but it's underspecified and marked obsolete by POSIX.
		 */
		if (posix_spawn(&sp->pid, path, &file_actions, &attr, argv, envp) < 0)
			goto fail;
		elog(LOG, "OpenSubprocess pid %d", sp->pid);

		/* Clean up temporary arguments. */
		posix_spawnattr_destroy(&attr);
		posix_spawn_file_actions_destroy(&file_actions);

		/* Keep the appropriate pipe end in the parent. */
		if (flags & SUBPROCESS_READ)
		{
			sp->pipe_fd = pipe_fds[flags & SUBPROCESS_READ ? 0 : 1];
			close(pipe_fds[flags & SUBPROCESS_READ ? 1 : 0]);
			ReleaseExternalFD();
		}

		dlist_push_tail(&subprocess_table, &sp->subprocess_table_node);
		break;

fail:
		elog(LOG, "fail!");
		save_errno = errno;
		if (have_attr)
			posix_spawnattr_destroy(&attr);
		if (have_file_actions)
			posix_spawn_file_actions_destroy(&file_actions);
		if (have_pipe)
		{
			close(pipe_fds[0]);
			close(pipe_fds[1]);
		}
		for (int i = 0; i < external_fds; ++i)
			ReleaseExternalFD();
		free(sp);
		sp = NULL;
		errno = save_errno;
	} while (0);
	UnlockSubprocessTable();

	/*
	 * Remove the SIGCHLD handler if we failed to spawn a subprocess, and no
	 * others subprocess are running.
	 */
	AdjustSubprocessExitHandler(false);
#else
	do
	{
		/* XXX windows CreateProcess() */
	} while (0);
#endif

	if (sp)
	{
		sp->resowner = CurrentResourceOwner;
		ResourceOwnerRemember(sp->resowner,
							  PointerGetDatum(sp),
							  &subprocess_resowner_desc);
	}

	return sp;
}

/*
 * Retrieve a subprocess's exit status, if it has finished running.  The latch
 * of the backend that called OpenSubprocess() will be set when the status is
 * available.  Returns false if the subprocess is still running.
 */
bool
GetSubprocessExitStatus(Subprocess *sp, int *exit_status)
{
	bool		result = false;

	Assert(sp->owner_procno == MyProcNumber);

	LockSubprocess(sp);			/* Windows only */
	if (sp->status == SUBPROCESS_STATUS_REAPED)
	{
		*exit_status = sp->exit_status;
		result = true;
	}
	UnlockSubprocess(sp);

	return result;
}

/*
 * Close a subprocess.  If the process hasn't finished running yet, it is
 * terminated and the exit status is lost.
 *
 * Unlike pclose(), control is returned without waiting for the subprocess to
 * exit, if WaitSubprocess() hasn't been called.  Normally that only happens
 * if the subprocess is closed during error cleanup.  The alternative would be
 * for a process that refuses to exit to hold up cancellation, which would be
 * complicated by the fact that error cleanup paths also hold interrupts,
 * needed to handle SIGCHLD notification.
 *
 * To avoid accumulating lingering subprocesses that are no longer of interest
 * after an error has been raised, we close any connected pipe and send
 * SIGTERM.  In that case, the waitpid() step is deferred without blocking
 * progress.
 */
void
CloseSubprocess(Subprocess *sp)
{
	bool		unreferenced = false;

	Assert(sp->owner_procno == MyProcNumber);

	LockSubprocessTable();
	LockSubprocess(sp);
	if (sp->status == SUBPROCESS_STATUS_RUNNING)
	{
		/*
		 * The object won't be freed until it is reaped, which should
		 * hopefully be pretty soon after we terminate it.  This case is
		 * expected when the subprocess is closed by the resource owner during
		 * error cleanup.
		 */
		sp->status = SUBPROCESS_STATUS_ABANDONED;

#ifndef WIN32
		/* Shut down our end of the pipe. */
		if (sp->flags)
		{
			close(sp->pipe_fd);
			ReleaseExternalFD();
		}

		/*
		 * SIGTERM is probably better than SIGKILL, because a program that
		 * itself created children might want to shut those down too.
		 */
		kill(sp->pid, SIGTERM);
#else
		/* Terminate the process and any children it has created. */
		CloseHandle(sp->job_handle);
#endif
	}
	else if (sp->status == SUBPROCESS_STATUS_REAPED)
	{
		/* Common case: freed immediately. */
		ForgetSubprocess(sp);
		unreferenced = true;
		if (sp->resowner)
			ResourceOwnerForget(sp->resowner,
								PointerGetDatum(sp),
								&subprocess_resowner_desc);
	}
	else
	{
		Assert(false);
	}
	UnlockSubprocess(sp);
	UnlockSubprocessTable();

	if (unreferenced)
		free(sp);
}

/*
 * Like ReadSubprocess(), but wait until a minimum number of bytes or EOF is
 * received.
 */
ssize_t
ReadSubprocessAtLeast(Subprocess *sp, void *buffer, size_t minsize, size_t maxsize)
{
	ssize_t		nbytes = 0;

	Assert(minsize <= maxsize);

	while (nbytes < minsize)
	{
		ssize_t		chunk_nbytes;

		Assert(maxsize > 0);

		chunk_nbytes = ReadSubprocess(sp, buffer, maxsize);
		if (chunk_nbytes < 0)
			return -1;	/* note: may discard data preceding error */
		if (chunk_nbytes == 0)
			break;

		nbytes += chunk_nbytes;
		if (chunk_nbytes == maxsize)
			break;

		buffer = (char *) buffer + chunk_nbytes;
		maxsize -= chunk_nbytes;
	}

	return nbytes;
}

/*
 * Read from a subprocess opened with SUBPROCESS_READ, blocking until data is
 * available if necessary, but also processing interrupts, Interrupts might
 * cause non-local exit by raising an error.  Returns -1 and sets errno on
 * error.
 */
ssize_t
ReadSubprocess(Subprocess *sp, void *buffer, size_t size)
{
	ssize_t		nbytes;
#ifndef WIN32
	bool		skip_internal_buffer;
#endif

	Assert(sp->flags & SUBPROCESS_READ);

	/* Do we already have some buffered data to copy out? */
	if (sp->buffer_index < sp->buffer_size)
	{
		nbytes = Min(sp->buffer_size - sp->buffer_index, size);
		memcpy(buffer, &sp->buffer[sp->buffer_index], nbytes);
		sp->buffer_index += nbytes;
		return nbytes;
	}

#ifndef WIN32
	/*
	 * If the caller asked for is a large percentage of our internal buffer's
	 * size (expected for COPY FROM, see SUBPROCESS_BUFFER_SIZE) then skip
	 * useless double-buffering here and read directly into the caller's
	 * buffer.  The internal buffer is only used to support small reads with
	 * more efficient large system calls.
	 */
	skip_internal_buffer = size >= SUBPROCESS_BUFFER_SIZE / 2;

retry:
	/* Read from pipe into the selected output buffer. */
	nbytes = read(sp->pipe_fd,
				  skip_internal_buffer ? buffer : sp->buffer,
				  skip_internal_buffer ? size : SUBPROCESS_BUFFER_SIZE);

	if (nbytes < 0)
	{
		if (errno == EAGAIN)
		{
			/* Wait for data in pipe, or latch to be set for interrupt. */
			if (WaitLatchOrSocket(MyLatch,
								  WL_LATCH_SET | WL_SOCKET_READABLE |
								  WL_EXIT_ON_PM_DEATH,
								  sp->pipe_fd,
								  0,
								  WAIT_EVENT_SUBPROCESS_READ) == WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
			goto retry;
		}
		if (errno == EINTR)
			goto retry;

		/* Report failure to caller, which should check errno. */
	}
	else if (!skip_internal_buffer)
	{
		/* Copy requested bytes from buffer, leaving the rest for later. */		
		sp->buffer_size = nbytes;
		if (size < nbytes)		
			nbytes = size;
		memcpy(buffer, sp->buffer, nbytes);
		sp->buffer_index = nbytes;
	}
#else
	/* XXX TODO */
#endif
	if (size != nbytes)
		elog(LOG, "ReadSubprocess -- asked for %zu, returning %zu", size, nbytes);
	return nbytes;
}

ssize_t
WriteSubprocess(Subprocess *sp, void *buffer, size_t size)
{
	ssize_t		nbytes;

#ifndef WIN32
	do
	{
		nbytes = write(sp->pipe_fd, buffer, size);

		if (nbytes == -1)
		{
			if (errno == EAGAIN)
			{
				WaitLatchOrSocket(MyLatch,
								  WL_LATCH_SET | WL_SOCKET_READABLE |
								  WL_EXIT_ON_PM_DEATH,
								  sp->pipe_fd,
								  0,
								  WAIT_EVENT_SUBPROCESS_READ);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			else if (errno == EINTR)
			{
				continue;
			}
		}
	}
	while (0);
#else
	/* XXX TODO */
#endif

	return nbytes;
}
