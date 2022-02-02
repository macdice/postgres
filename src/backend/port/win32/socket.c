/*-------------------------------------------------------------------------
 *
 * socket.c
 *	  Microsoft Windows Win32 Socket Functions
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/port/win32/socket.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* Undef the macros defined in win32.h, so we can access system functions */
#undef socket
#undef bind
#undef listen
#undef accept
#undef connect
#undef select
#undef recv
#undef send

/*
 * Convert the last socket error code into errno
 *
 * Note: where there is a direct correspondence between a WSAxxx error code
 * and a Berkeley error symbol, this mapping is actually a no-op, because
 * in win32_port.h we redefine the network-related Berkeley error symbols to
 * have the values of their WSAxxx counterparts.  The point of the switch is
 * mostly to translate near-miss error codes into something that's sensible
 * in the Berkeley universe.
 */
static void
TranslateSocketError(void)
{
	switch (WSAGetLastError())
	{
		case WSAEINVAL:
		case WSANOTINITIALISED:
		case WSAEINVALIDPROVIDER:
		case WSAEINVALIDPROCTABLE:
		case WSAEDESTADDRREQ:
			errno = EINVAL;
			break;
		case WSAEINPROGRESS:
			errno = EINPROGRESS;
			break;
		case WSAEFAULT:
			errno = EFAULT;
			break;
		case WSAEISCONN:
			errno = EISCONN;
			break;
		case WSAEMSGSIZE:
			errno = EMSGSIZE;
			break;
		case WSAEAFNOSUPPORT:
			errno = EAFNOSUPPORT;
			break;
		case WSAEMFILE:
			errno = EMFILE;
			break;
		case WSAENOBUFS:
			errno = ENOBUFS;
			break;
		case WSAEPROTONOSUPPORT:
		case WSAEPROTOTYPE:
		case WSAESOCKTNOSUPPORT:
			errno = EPROTONOSUPPORT;
			break;
		case WSAECONNABORTED:
			errno = ECONNABORTED;
			break;
		case WSAECONNREFUSED:
			errno = ECONNREFUSED;
			break;
		case WSAECONNRESET:
			errno = ECONNRESET;
			break;
		case WSAEINTR:
			errno = EINTR;
			break;
		case WSAENOTSOCK:
			errno = ENOTSOCK;
			break;
		case WSAEOPNOTSUPP:
			errno = EOPNOTSUPP;
			break;
		case WSAEWOULDBLOCK:
			errno = EWOULDBLOCK;
			break;
		case WSAEACCES:
			errno = EACCES;
			break;
		case WSAEADDRINUSE:
			errno = EADDRINUSE;
			break;
		case WSAEADDRNOTAVAIL:
			errno = EADDRNOTAVAIL;
			break;
		case WSAEHOSTDOWN:
			errno = EHOSTDOWN;
			break;
		case WSAEHOSTUNREACH:
		case WSAHOST_NOT_FOUND:
			errno = EHOSTUNREACH;
			break;
		case WSAENETDOWN:
			errno = ENETDOWN;
			break;
		case WSAENETUNREACH:
			errno = ENETUNREACH;
			break;
		case WSAENETRESET:
			errno = ENETRESET;
			break;
		case WSAENOTCONN:
		case WSAESHUTDOWN:
		case WSAEDISCON:
			errno = ENOTCONN;
			break;
		case WSAETIMEDOUT:
			errno = ETIMEDOUT;
			break;
		default:
			ereport(NOTICE,
					(errmsg_internal("unrecognized win32 socket error code: %d",
									 WSAGetLastError())));
			errno = EINVAL;
			break;
	}
}

static int
pgwin32_poll_signals(void)
{
	if (UNBLOCKED_SIGNAL_QUEUE())
	{
		pgwin32_dispatch_queued_signals();
		errno = EINTR;
		return 1;
	}
	return 0;
}

/*
 * Create a socket, setting it to overlapped and non-blocking
 */
SOCKET
pgwin32_socket(int af, int type, int protocol)
{
	SOCKET		s;

	errno = 0;
	s = socket(af, type, protocol);
	if (s == INVALID_SOCKET)
		TranslateSocketError();

	return s;
}

int
pgwin32_bind(SOCKET s, struct sockaddr *addr, int addrlen)
{
	int			res;

	res = bind(s, addr, addrlen);
	if (res < 0)
		TranslateSocketError();
	return res;
}

int
pgwin32_listen(SOCKET s, int backlog)
{
	int			res;

	res = listen(s, backlog);
	if (res < 0)
		TranslateSocketError();
	return res;
}

SOCKET
pgwin32_accept(SOCKET s, struct sockaddr *addr, int *addrlen)
{
	SOCKET		rs;

	/*
	 * Poll for signals, but don't return with EINTR, since we don't handle
	 * that in pqcomm.c
	 */
	pgwin32_poll_signals();

	rs = WSAAccept(s, addr, addrlen, NULL, 0);
	if (rs == INVALID_SOCKET)
	{
		TranslateSocketError();
		return INVALID_SOCKET;
	}
	return rs;
}


/* No signal delivery during connect. */
int
pgwin32_connect(SOCKET s, const struct sockaddr *addr, int addrlen)
{
	int			res;

	res = connect(s, addr, addrlen);
	if (res < 0)
		return -1;

	return res;
}

int
pgwin32_recv(SOCKET s, char *buf, int len, int f)
{
	int			res;

	res = recv(s, buf, len, f);
	if (res < 0)
		TranslateSocketError();
	return res;
}

int
pgwin32_send(SOCKET s, const void *buf, int len, int flags)
{
	int			res;

	res = send(s, buf, len, flags);
	if (res < 0)
		TranslateSocketError();
	return res;
}


/*
 * Wait for activity on one or more sockets.
 * While waiting, allow signals to run
 *
 * NOTE! Currently does not implement exceptfds check,
 * since it is not used in postgresql!
 */
int
pgwin32_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval *timeout)
{
	WSAEVENT	events[FD_SETSIZE * 2]; /* worst case is readfds totally
										 * different from writefds, so
										 * 2*FD_SETSIZE sockets */
	SOCKET		sockets[FD_SETSIZE * 2];
	int			numevents = 0;
	int			i;
	int			r;
	DWORD		timeoutval = WSA_INFINITE;
	FD_SET		outreadfds;
	FD_SET		outwritefds;
	int			nummatches = 0;

	Assert(exceptfds == NULL);

	if (pgwin32_poll_signals())
		return -1;

	FD_ZERO(&outreadfds);
	FD_ZERO(&outwritefds);

	/*
	 * Windows does not guarantee to log an FD_WRITE network event indicating
	 * that more data can be sent unless the previous send() failed with
	 * WSAEWOULDBLOCK.  While our caller might well have made such a call, we
	 * cannot assume that here.  Therefore, if waiting for write-ready, force
	 * the issue by doing a dummy send().  If the dummy send() succeeds,
	 * assume that the socket is in fact write-ready, and return immediately.
	 * Also, if it fails with something other than WSAEWOULDBLOCK, return a
	 * write-ready indication to let our caller deal with the error condition.
	 */
	if (writefds != NULL)
	{
		for (i = 0; i < writefds->fd_count; i++)
		{
			char		c;
			WSABUF		buf;
			DWORD		sent;

			buf.buf = &c;
			buf.len = 0;

			r = WSASend(writefds->fd_array[i], &buf, 1, &sent, 0, NULL, NULL);
			if (r == 0 || WSAGetLastError() != WSAEWOULDBLOCK)
				FD_SET(writefds->fd_array[i], &outwritefds);
		}

		/* If we found any write-ready sockets, just return them immediately */
		if (outwritefds.fd_count > 0)
		{
			memcpy(writefds, &outwritefds, sizeof(fd_set));
			if (readfds)
				FD_ZERO(readfds);
			return outwritefds.fd_count;
		}
	}


	/* Now set up for an actual select */

	if (timeout != NULL)
	{
		/* timeoutval is in milliseconds */
		timeoutval = timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
	}

	if (readfds != NULL)
	{
		for (i = 0; i < readfds->fd_count; i++)
		{
			events[numevents] = WSACreateEvent();
			sockets[numevents] = readfds->fd_array[i];
			numevents++;
		}
	}
	if (writefds != NULL)
	{
		for (i = 0; i < writefds->fd_count; i++)
		{
			if (!readfds ||
				!FD_ISSET(writefds->fd_array[i], readfds))
			{
				/* If the socket is not in the read list */
				events[numevents] = WSACreateEvent();
				sockets[numevents] = writefds->fd_array[i];
				numevents++;
			}
		}
	}

	for (i = 0; i < numevents; i++)
	{
		int			flags = 0;

		if (readfds && FD_ISSET(sockets[i], readfds))
			flags |= FD_READ | FD_ACCEPT | FD_CLOSE;

		if (writefds && FD_ISSET(sockets[i], writefds))
			flags |= FD_WRITE | FD_CLOSE;

		if (WSAEventSelect(sockets[i], events[i], flags) != 0)
		{
			TranslateSocketError();
			/* release already-assigned event objects */
			while (--i >= 0)
				WSAEventSelect(sockets[i], NULL, 0);
			for (i = 0; i < numevents; i++)
				WSACloseEvent(events[i]);
			return -1;
		}
	}

	events[numevents] = pgwin32_signal_event;
	r = WaitForMultipleObjectsEx(numevents + 1, events, FALSE, timeoutval, TRUE);
	if (r != WAIT_TIMEOUT && r != WAIT_IO_COMPLETION && r != (WAIT_OBJECT_0 + numevents))
	{
		/*
		 * We scan all events, even those not signaled, in case more than one
		 * event has been tagged but Wait.. can only return one.
		 */
		WSANETWORKEVENTS resEvents;

		for (i = 0; i < numevents; i++)
		{
			ZeroMemory(&resEvents, sizeof(resEvents));
			if (WSAEnumNetworkEvents(sockets[i], events[i], &resEvents) != 0)
				elog(ERROR, "failed to enumerate network events: error code %d",
					 WSAGetLastError());
			/* Read activity? */
			if (readfds && FD_ISSET(sockets[i], readfds))
			{
				if ((resEvents.lNetworkEvents & FD_READ) ||
					(resEvents.lNetworkEvents & FD_ACCEPT) ||
					(resEvents.lNetworkEvents & FD_CLOSE))
				{
					FD_SET(sockets[i], &outreadfds);

					nummatches++;
				}
			}
			/* Write activity? */
			if (writefds && FD_ISSET(sockets[i], writefds))
			{
				if ((resEvents.lNetworkEvents & FD_WRITE) ||
					(resEvents.lNetworkEvents & FD_CLOSE))
				{
					FD_SET(sockets[i], &outwritefds);

					nummatches++;
				}
			}
		}
	}

	/* Clean up all the event objects */
	for (i = 0; i < numevents; i++)
	{
		WSAEventSelect(sockets[i], NULL, 0);
		WSACloseEvent(events[i]);
	}

	if (r == WSA_WAIT_TIMEOUT)
	{
		if (readfds)
			FD_ZERO(readfds);
		if (writefds)
			FD_ZERO(writefds);
		return 0;
	}

	/* Signal-like events. */
	if (r == WAIT_OBJECT_0 + numevents || r == WAIT_IO_COMPLETION)
	{
		pgwin32_dispatch_queued_signals();
		errno = EINTR;
		if (readfds)
			FD_ZERO(readfds);
		if (writefds)
			FD_ZERO(writefds);
		return -1;
	}

	/* Overwrite socket sets with our resulting values */
	if (readfds)
		memcpy(readfds, &outreadfds, sizeof(fd_set));
	if (writefds)
		memcpy(writefds, &outwritefds, sizeof(fd_set));
	return nummatches;
}
