/*-------------------------------------------------------------------------
 *
 * socket.c
 *	  Microsoft Windows Win32 Socket Functions
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
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
#undef recv
#undef send

/*
 * Blocking socket functions implemented so they listen on both
 * the socket and the signal event, required for signal handling.
 */

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
	unsigned long on = 1;

	s = WSASocket(af, type, protocol, NULL, 0, 0);
	if (s == INVALID_SOCKET)
	{
		TranslateSocketError();
		return INVALID_SOCKET;
	}

	if (ioctlsocket(s, FIONBIO, &on))
	{
		TranslateSocketError();
		closesocket(s);
		return INVALID_SOCKET;
	}
	errno = 0;

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
	int			r;

	r = WSAConnect(s, addr, addrlen, NULL, NULL, NULL, NULL);
	if (r == 0)
		return 0;

	TranslateSocketError();
	return -1;
}

int
pgwin32_recv(SOCKET s, char *buf, int len, int f)
{
	WSABUF		wbuf;
	int			r;
	DWORD		b;
	DWORD		flags = f;

	if (pgwin32_poll_signals())
		return -1;

	wbuf.len = len;
	wbuf.buf = buf;

	r = WSARecv(s, &wbuf, 1, &b, &flags, NULL, NULL);
	if (r != SOCKET_ERROR)
		return b;				/* success */

	if (WSAGetLastError() != WSAEWOULDBLOCK)
	{
		TranslateSocketError();
		return -1;
	}

	/*
	 * No data received, and we only support "emulated non-blocking mode", so return
	 * indicating that we'd block if we were to continue.
	 */
	errno = EWOULDBLOCK;
	return -1;
}

/*
 * The second argument to send() is defined by SUS to be a "const void *"
 * and so we use the same signature here to keep compilers happy when
 * handling callers.
 *
 * But the buf member of a WSABUF struct is defined as "char *", so we cast
 * the second argument to that here when assigning it, also to keep compilers
 * happy.
 */

int
pgwin32_send(SOCKET s, const void *buf, int len, int flags)
{
	WSABUF		wbuf;
	int			r;
	DWORD		b;

	if (pgwin32_poll_signals())
		return -1;

	wbuf.len = len;
	wbuf.buf = (char *) buf;

	/*
	 * Readiness of socket to send data to UDP socket may be not true: socket
	 * can become busy again! So loop until send or error occurs.
	 */
	for (;;)
	{
		r = WSASend(s, &wbuf, 1, &b, flags, NULL, NULL);
		if (r != SOCKET_ERROR && b > 0)
			/* Write succeeded right away */
			return b;

		if (r == SOCKET_ERROR &&
			WSAGetLastError() != WSAEWOULDBLOCK)
		{
			TranslateSocketError();
			return -1;
		}

		/*
		 * No data sent, and we only support "emulated non-blocking mode", so
		 * return indicating that we'd block if we were to continue.
		 */
		errno = EWOULDBLOCK;
		return -1;
	}

	return -1;
}
