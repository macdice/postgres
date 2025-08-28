#ifndef SUBPROCESS_H
#define SUBPROCESS_H

/* OpenSubprocess() flags controlling pipe direction. */
#define SUBPROCESS_READ		1
#define SUBPROCESS_WRITE	2

struct Subprocess;
typedef struct Subprocess Subprocess;

/* Simple system()-style interface. */
extern int	RunSubprocess(const char *shell_command, char *const envp[]);

/* Simple popen()-style interface. */
extern Subprocess *OpenSubprocess(const char *shell_command,
								  int flags,
								  char *const envp[]);
extern ssize_t ReadSubprocess(Subprocess *sp, void *buffer, size_t size);
extern ssize_t WriteSubprocess(Subprocess *sp, void *buffer, size_t size);
extern void CloseSubprocess(Subprocess *sp);

/* Access exit status, with or without waiting. */
extern int	WaitSubprocess(Subprocess *sp);
extern bool GetSubprocessExitStatus(Subprocess *sp, int *exit_status);

#ifndef WIN32
/* SIGCHLD handler support. */
extern volatile sig_atomic_t SubprocessExitPending;
extern void ProcessSubprocessExit(void);
#endif

#endif
