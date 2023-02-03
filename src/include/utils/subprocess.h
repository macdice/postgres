#ifndef SUBPROCESS_H
#define SUBPROCESS_H

typedef struct Subprocess
{
	pid_t		pid;
	int			canary_pipe[2];
} Subprocess;

extern pid_t start_shell_subprocess(Subprocess *sp, const char *shell_command);
extern int wait_latch_or_subprocess(Latch *latch,
									Subprocess *sp,
									int *wstatus,
									int wait_event);
#endif
