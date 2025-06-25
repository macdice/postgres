#ifndef FD_REGISTRY_H
#define FD_REGISTRY_H

/* Opaque handle used to refer to registered file descriptors. */
typedef struct fd_registry_handle
{
	pid_t		pid;
	pgsocket	pid_fd;
} fd_registry_handle;

extern void fd_registry_init(void);

extern bool fd_registry_insert(int fd, fd_registry_handle *handle);
extern bool fd_registry_insert_socket(pgsocket fd, fd_registry_handle *handle);
extern bool fd_registry_delete(const fd_registry_handle *handle);
extern int fd_registry_dup(const fd_registry_handle *handle);
extern pgsocket fd_registry_dup_socket(const fd_registry_handle *handle);

#endif
