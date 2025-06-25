/*
 * Mechanism allowing file descriptors to be passed between processes.
 *
 * The portable Unix implementation uses a socket pair holding SCM_RIGHTS
 * messages as a container.  It is not very efficient.
 *
 * XXX For recent Linux, we could use pidfd_getfd() instead.  We could perform
 * a runtime test and remember whether the kernel has it available, and in that
 * case we could skip the socketpair and duplicate fds directly from the
 * creating process, given some lifetime management rules.
 *
 * XXX For Windows, we should be able to use DuplicateHandle() or
 * DuplicateSocket(), hence the need for different entry points for sockets and
 * other fds, hence different entry points in this API.
 */

#include "c.h"

#include "miscadmin.h"
#include "port/pg_iovec.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/elog.h"
#include "utils/fd_registry.h"

#ifndef WIN32

#include <sys/socket.h>

/*
 * XXX Think about how this relates to the socket buffer size, ie how big a
 * message we can write into the socket without blocking.  Or figure out how to
 * remove this limit and set the socket buffer size dynamically?  For
 * pidfd_getfd() and Windows we don't need a limit at all.
 */
#define FD_REGISTRY_MAX_SIZE 16

#define FD_REGISTRY_SEND 0
#define FD_REGISTRY_RECV 1

typedef struct fd_registry {
	int count;
	fd_registry_handle handles[FD_REGISTRY_MAX_SIZE];
} fd_registry;

static int fd_registry_socket[2] = {-1, -1};

/*
 * Store the table and descriptors in the socket, ready to be imported by any
 * backend.
 */
static void
fd_registry_export(fd_registry *table, int *local_fds)
{
	struct msghdr msghdr = {0};
	struct iovec iov;
	struct cmsghdr *cmsghdr;
	union
	{
		struct cmsghdr cmsghdr;
		char buffer[CMSG_SPACE(sizeof(local_fds[0]) * FD_REGISTRY_MAX_SIZE)];
	} u;
	ssize_t sent;

	Assert(LWLockHeldByMeInMode(FdRegistryLock, LW_EXCLUSIVE));

	/* Send the count without attached fds so the importer can reserve fds. */
	sent = send(fd_registry_socket[FD_REGISTRY_SEND],
				&table->count,
				sizeof(table->count),
				0);
	if (sent < 0)
		elog(PANIC, "could not write to fd_registry_socket: %m");
	else if (sent < sizeof(table->count))
		elog(PANIC, "could not write to fd_registry_socket: sent only %zu of %zu bytes",
			 sent, sizeof(table->count));

	/* No descriptors?  Then we're done. */
	if (table->count == 0)
		return;

	/* Prepare to send the table itself. */
	iov.iov_base = &table->handles[0];
	iov.iov_len = sizeof(table->handles[0]) * table->count;
	msghdr.msg_iov = &iov;
	msghdr.msg_iovlen = 1;

	/* Attach local_fds to the message for the kernel to duplicate. */
	msghdr.msg_control = u.buffer;
	msghdr.msg_controllen = CMSG_SPACE(sizeof(local_fds[0]) * table->count);
	cmsghdr = CMSG_FIRSTHDR(&msghdr);
	cmsghdr->cmsg_level = SOL_SOCKET;
	cmsghdr->cmsg_type = SCM_RIGHTS;
	cmsghdr->cmsg_len = CMSG_LEN(sizeof(local_fds[0]) * table->count);
	memcpy(CMSG_DATA(cmsghdr), local_fds, sizeof(local_fds[0]) * table->count);

	/* Send handles and descriptors. */
	sent = sendmsg(fd_registry_socket[FD_REGISTRY_SEND], &msghdr, 0);
	if (sent < 0)
		elog(PANIC, "could not send to fd_registry_socket: %m");
	else if (sent < iov.iov_len)
		elog(PANIC, "could not send to fd_registry_socket: sent only %zu of %zu bytes",
			 sent, iov.iov_len);
}

/*
 * Read table from the socket, where it was stored by the last backend to
 * modify it.  Also receive duplicates of all referenced descriptors in
 * local_fds, which must have space for FD_REGISTRY_MAX_SIZE descriptors.  The
 * actual number filled in is written to table->count, and the caller must
 * close any that it doesn't want.  The caller should also export a new table
 * after any desired modifications.
 */
static void
fd_registry_import(fd_registry *table, int *local_fds)
{
	struct msghdr msghdr;
	struct iovec iov;
	struct cmsghdr *cmsghdr;
	union
	{
		struct cmsghdr cmsghdr;
		char buffer[CMSG_SPACE(sizeof(local_fds[0]) * FD_REGISTRY_MAX_SIZE)];
	} u;
	ssize_t expected;
	ssize_t received;

	Assert(LWLockHeldByMeInMode(FdRegistryLock, LW_EXCLUSIVE));

	/* Receive table->count. */
	received = recv(fd_registry_socket[FD_REGISTRY_RECV], &table->count, sizeof(table->count), 0);
	if (received < 0)
		elog(PANIC, "could not receive from fd_registry_socket: %m");
	else if (received < sizeof(table->count))
		elog(PANIC,
			 "could not receive from fd_registry_socket: recieved only %zu bytes but expected at least %zu",
			 received, sizeof(table->count));
	else if (table->count < 0 || table->count > FD_REGISTRY_MAX_SIZE)
		elog(PANIC,
			 "receive out of range count from fd_registry_socket: %d",
			 table->count);

	/* No descriptors?  Then we're done. */
	if (table->count == 0)
		return;

	/* Make sure we have room to accept table->count descriptors. */
	for (int i = 0; i < table->count; i++)
		ReserveExternalFD();	/* XXX ??? */

	/* Receive the handles and corresponding descriptors. */
	iov.iov_base = &table->handles;
	iov.iov_len = sizeof(table->handles[0]) * table->count;
	msghdr.msg_iov = &iov;
	msghdr.msg_iovlen = 1;
	msghdr.msg_control = u.buffer;
	msghdr.msg_controllen = sizeof(u.buffer);
	received = recvmsg(fd_registry_socket[FD_REGISTRY_RECV], &msghdr, 0);
	if (received < 0)
		elog(PANIC, "could not receive from fd_registry_socket: %m");
	expected = sizeof(table->handles[0]) * table->count;
	if (received != expected)
		elog(PANIC,
			 "could not receive from fd_registry_socket: recieved %zu bytes but expected %zu",
			 received, expected);

	/* Copy out the duplicated descriptors. */
	/* XXX check that the size is right? */
	cmsghdr = CMSG_FIRSTHDR(&msghdr);
	memcpy(local_fds, CMSG_DATA(cmsghdr), sizeof(local_fds[0]) * table->count);
}

#endif

/*
 * Called by the postmaster to initialize the file descriptor registry.
 */
void
fd_registry_init(void)
{
	fd_registry empty = {0};

	/*
	 * Must be called in postmaster so that the Unix domain socket is
	 * inherited by all backends.
	 */
	Assert(!IsUnderPostmaster);

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd_registry_socket) < 0)
		elog(ERROR, "could not create socketpair: %m");

	/*
	 * Make both ends non-blocking, so that we can panic instead of hanging if
	 * our reasoning [XXX FIXME] about buffer space is incorrect or the pipe is
	 * unexpectedly empty.
	 */
	for (int i = 0; i < lengthof(fd_registry_socket); ++i)
	{
		int flags = fcntl(fd_registry_socket[i], F_GETFL, 0);
		if (flags < 0)
			elog(PANIC, "fcntl(F_GETFL) failed: %m");
		if (fcntl(fd_registry_socket[i], F_SETFL, flags | O_NONBLOCK) < 0)
			elog(PANIC, "fcntl(F_SETFL) failed: %m");
	}

	/* Store an empty table in the socket. */
	LWLockAcquire(FdRegistryLock, LW_EXCLUSIVE);
	fd_registry_export(&empty, NULL);
	LWLockRelease(FdRegistryLock);
}

/*
 * Make a file descriptor available to other backends.  Returns -1 on error and
 * leaves errno set.  Returns true if there was space in the registry.
 */
bool
fd_registry_insert(int fd, fd_registry_handle *handle)
{
#ifdef WIN32
	/* XXX write me */
#else
	bool result;
	fd_registry table;
	int local_fds[FD_REGISTRY_MAX_SIZE];

	LWLockAcquire(FdRegistryLock, LW_EXCLUSIVE);
	fd_registry_import(&table, local_fds);
	if (table.count < FD_REGISTRY_MAX_SIZE)
	{
		/* Append new fd to the table. */
		handle->pid = MyProcPid;
		handle->pid_fd = fd;
		table.handles[table.count] = *handle;
		local_fds[table.count] = fd;
		table.count++;
		result = true;
	}
	else
	{
		/* No space for another fd. */
		result = false;
	}
	fd_registry_export(&table, local_fds);
	LWLockRelease(FdRegistryLock);

	/* Close all imported fds, but not the one we possibly just inserted. */
	for (int i = 0; i < table.count - result ? 1 : 0; i++)
		close(local_fds[i]);

	return result;
#endif
}

/*
 * Variant of fd_registry_insert() for sockets.
 */
bool
fd_registry_insert_socket(pgsocket fd, fd_registry_handle *handle)
{
#ifdef WIN32
	/* XXX write me */
#else
	/* Everything is a file */
	return fd_registry_insert(fd, handle);
#endif
}

bool
fd_registry_delete(const fd_registry_handle *handle)
{
	bool result = false;
	fd_registry table;
	int local_fds[FD_REGISTRY_MAX_SIZE];

	LWLockAcquire(FdRegistryLock, LW_EXCLUSIVE);
	fd_registry_import(&table, local_fds);
	for (int i = 0; i < table.count; i++)
	{
		/* Is this it? */
		if (table.handles[i].pid == handle->pid &&
			table.handles[i].pid_fd == handle->pid_fd)
		{
			/* Close the imported fd we're removing. */
			close(local_fds[i]);
			/* Final handle moves to this position. */
			if (i < table.count - 1)
			{
				table.handles[i] = table.handles[table.count - 1];
				local_fds[i] = local_fds[table.count - 1];
			}
			Assert(table.count > 0);
			table.count--;
			result = true;
			break;
		}
	}
	fd_registry_export(&table, local_fds);
	LWLockRelease(FdRegistryLock);

	/* Close all imported fds. */
	for (int i = 0; i < table.count; i++)
		close(local_fds[i]);

	return result;
}

int
fd_registry_dup(const fd_registry_handle *handle)
{
#ifdef WIN32
	/* XXX write me */
#else
	int result = -1;
	fd_registry table;
	int local_fds[FD_REGISTRY_MAX_SIZE];

	LWLockAcquire(FdRegistryLock, LW_EXCLUSIVE);
	fd_registry_import(&table, local_fds);
	for (int i = 0; i < table.count; i++)
	{
		/* Is this it? */
		if (table.handles[i].pid == handle->pid &&
			table.handles[i].pid_fd == handle->pid_fd)
		{
			result = local_fds[i];
			break;
		}
	}
	fd_registry_export(&table, local_fds);
	LWLockRelease(FdRegistryLock);

	/* Close all other imported fds. */
	for (int i = 0; i < table.count; i++)
		if (local_fds[i] != result)
			close(local_fds[i]);

	return result;
#endif
}

int
fd_registry_dup_socket(const fd_registry_handle *handle)
{
#ifdef WIN32
	/* XXX write me */
#else
	return fd_registry_dup(handle);
#endif
}
