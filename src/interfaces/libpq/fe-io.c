/*-------------------------------------------------------------------------
 *
 * fe-io.c
 *	  functions related to raw socket IO.
 *
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-io.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "libpq-int.h"

#ifdef WIN32
#include "win32.h"
#else
#include <sys/socket.h>
#endif

/*
 * Perform a recv() call, or queue up a request for the client of libpq do so
 * if external IO is configured.
 */
int
pqio_recv(PGconn *conn, void *ptr, size_t len, int flags)
{
	if (conn->io_external)
	{
		/* Is some IO we asked for unexpectedly still not finished? */
		if (unlikely(conn->io.error == EINPROGRESS))
		{
			errno = EWOULDBLOCK;
			return -1;
		}

		/*
		 * See if we already asked for this exact recv() to start.  This only
		 * works as long as we can trust that all callers will always come
		 * back here again on EAGAIN/EWOULDBLOCK without clobbering the socket
		 * or the buffer between the calls!
		 */
		if (conn->io.op == PQIO_OP_RECV &&
			conn->io.u.recv_args.s == conn->sock &&
			conn->io.u.recv_args.buf == ptr &&
			conn->io.u.recv_args.len == len &&
			conn->io.u.recv_args.flags == flags)
		{
			conn->io.op = PQIO_OP_NONE;
			errno = conn->io.error;
			return conn->io.result;
		}

		/* Ask for this recv() to start. */
		conn->io.op = PQIO_OP_RECV;
		conn->io.u.recv_args.s = conn->sock;
		conn->io.u.recv_args.buf = ptr;
		conn->io.u.recv_args.len = len;
		conn->io.u.recv_args.flags = flags;
		errno = EWOULDBLOCK;
		return -1;
	}

	return recv(conn->sock, ptr, len, flags);
}

/*
 * Perform a send() call, or queue up a request for the client of libpq do so
 * if external IO is configured.
 */
int
pqio_send(PGconn *conn, const void *ptr, size_t len, int flags)
{
	if (conn->io_external)
	{
		/* Is some IO we asked for unexpectedly still not finished? */
		if (unlikely(conn->io.error == EINPROGRESS))
		{
			errno = EWOULDBLOCK;
			return -1;
		}

		/*
		 * See if we already asked for this exact send() to start.  This only
		 * works as long as we can trust that all callers will always come
		 * back here again on EAGAIN/EWOULDBLOCK without clobbering the socket
		 * or the buffer between the calls!
		 */
		if (conn->io.op == PQIO_OP_SEND &&
			conn->io.u.send_args.s == conn->sock &&
			conn->io.u.send_args.buf == ptr &&
			conn->io.u.send_args.len == len &&
			conn->io.u.send_args.flags == flags)
		{
			conn->io.op = PQIO_OP_NONE;
			errno = conn->io.error;
			return conn->io.result;
		}

		/* Ask for this send() to start. */
		conn->io.op = PQIO_OP_SEND;
		conn->io.u.send_args.s = conn->sock;
		conn->io.u.send_args.buf = ptr;
		conn->io.u.send_args.len = len;
		conn->io.u.send_args.flags = flags;
		errno = EWOULDBLOCK;
		return -1;
	}

	return send(conn->sock, ptr, len, flags);
}

/*
 * Perform a connect() call, or queue up a request for the client of libpq do
 * so if external IO is configured.
 */
int
pqio_connect(PGconn *conn, const struct sockaddr *name, socklen_t namelen)
{
	if (conn->io_external)
	{
		/* Is some IO we asked for unexpectedly still not finished? */
		if (unlikely(conn->io.error == EINPROGRESS))
		{
			errno = EIO;			/* XXX we're out of sync and can't recover? */
			return -1;
		}

		/*
		 * libpq always calls connect() in nonblocking mode, and you use
		 * getsockopt() to read results, so this has a different form than the
		 * recv()/send() code and only works for the very specific way that
		 * fe-connect.c is coded.
		 */

		/* Ask for this connect() to start. */
		conn->io.op = PQIO_OP_CONNECT;
		conn->io.u.connect_args.s = conn->sock;
		conn->io.u.connect_args.name = name;
		conn->io.u.connect_args.namelen = namelen;
		errno = EINPROGRESS;
		return -1;
	}

	return connect(conn->sock, name, namelen);
}

void
PQsetExternalIo(PGconn *conn, int value)
{
	conn->io_external = value;
}

int
PQpendingIo(PGconn *conn, PQIO **ios, size_t len)
{
	/* Output buffer had better have space. */
	if (len < 1)
		return 0;
	
	/*
	 * XXX We might be able to report more than one at a time, to start a
	 * send() and recv() for the expected response at the same time (perhaps
	 * the client can start both of those with one system call).  For now it's
	 * 0 or 1.
	 */
	if (conn->io.error == EINPROGRESS)
	{
		ios[0] = &conn->io;
		return 1;
	}

	return 0;
}
