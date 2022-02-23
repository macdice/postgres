#include "port/pg_socket.h"

/* We do not want the legacy Windows wrappers here. */
#undef send
#undef recv

#ifdef WIN32
#include <winsock2.h>
#else
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

/*
 * An object holding a socket, and in the case of Windows, associated state.
 */
struct Socket
{
	pg_socket_descriptor_t descriptor;	/* Kernel descriptor/handle */

#ifdef WIN32
	HANDLE		event_handle;			/* Event for lifetime of descriptor */
	int			selected_flags;			/* Most recent WSAEventSelect() */
	int			received_flags;			/* Flags for edge->level conversion */
	bool		blocking;				/* Is descriptor blocking? */
#endif
};

#ifdef WIN32
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
#if !defined(FRONTEND)
			ereport(NOTICE,
					(errmsg_internal("unrecognized win32 socket error code: %d",
									 WSAGetLastError())));
#endif
			errno = EINVAL;
			break;
	}
}
#endif

#if defined(WIN32) && !defined(FRONTEND)
/*
 * Wait for condition 'flag' to be reported by the socket's kernel event, or a
 * simulated signal to arrive.  Return true if the the event arrived,
 * otherwise set errno and return false.
 */
static bool
pg_socket_wait(Socket *sock, int flag)
{
	HANDLE		events[2];
	int			rc;

	events[0] = pgwin32_signal_event;
	events[1] = sock->event_handle;

	/* Adjust the selected event, if necessary. */
	if (sock->selected_flags != flag &&
		WSAEventSelect(sock->descriptor, sock->event_handle, flag) != 0)
	{
		sock->selected_flags = 0;
		TranslateSocketError();
		return false;
	}
	sock->selected_flags = flag;

	/* Wait for pseudo-signal or WSA event. */
	rc = WaitForMultipleObjects(lengthof(events), events, false, INFINITE);
	if (rc == WAIT_OBJECT_0)
	{
		pgwin32_dispatch_queued_signals();
		errno = EINTR;
		return false;
	}
	else if (rc == WAIT_OBJECT_0 + 1)
	{
		return true;
	}
	else
	{
		ereport(ERROR,
				(errmsg_internal("unrecognized return value from WaitForMultipleObjects: %d (error code %lu)", rc, GetLastError())));
	}
}
#endif

Socket *
pg_socket_open(pg_socket_descriptor_t descriptor)
{
	Socket *sock;

	sock = (Socket *) malloc(sizeof(*sock));
	if (!sock)
	{
		errno = ENOMEM;
		return NULL;
	}

	sock->descriptor = descriptor;

#ifdef WIN32
	/* Create the event that will live as long as the socket. */
	if ((sock->event_handle = WSACreateEvent()) == WSA_INVALID_EVENT)
	{
		int errno_save;

		TranslateSocketError();
		errno_save = errno;
		free(sock);
		errno = errno_save;

		return NULL;
	}

	/* Put the socket into non-blocking mode. */
	{
		unsigned long value = 1;
		int			errno_save;

		if (ioctlsocket(descriptor, FIONBIO, &value) != 0)
		{
			TranslateSocketError();
			errno_save = errno;
			WSACloseEvent(sock->event_handle);
			free(sock);
			errno = errno_save;

			return NULL;
		}
	}
	sock->selected_flags = 0;
	sock->received_flags = 0;
	sock->blocking = true;		/* Initially we do blocking I/O. */
#endif

	return sock;
}

void
pg_socket_close(Socket *sock)
{
#ifdef WIN32
	WSACloseEvent(sock->event_handle);
#endif
	closesocket(sock->descriptor);
	free(sock);
}

/*
 * Equivalent of POSIX send().  On Windows, in backend code, this also waits
 * up for simulated signals.
 */
ssize_t
pg_socket_send(Socket *sock, const void *buf, size_t len, int flags)
{
	ssize_t		result;

#ifdef WIN32
	/*
	 * send() is one of the functions that re-enables the FD_WRITE event, so
	 * we can clear it from our set of 'sticky' events used for edge->level
	 * conversion.
	 *
	 * https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsaeventselect
	 */
	sock->received_flags &= ~FD_WRITE;
#endif

	do
	{
		result = send(sock->descriptor, buf, len, flags);
#if defined(WIN32)
		if (result < 0)
		{
			TranslateSocketError();
#if !defined(FRONTEND)
			if (errno == EWOULDBLOCK &&
				sock->blocking &&
				pg_socket_wait(sock, FD_WRITE))
				continue;
#endif
		}
#endif
	} while (false);

	return result;
}

ssize_t
pg_socket_recv(Socket *sock, void *buf, size_t len, int flags)
{
	ssize_t		result;

#ifdef WIN32
	/*
	 * recv() is one of the functions that re-enables the FD_READ event, so we
	 * can clear it from our set of 'sticky' events used for edge->level
	 * conversion.
	 *
	 * https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsaeventselect
	 */
	sock->received_flags &= ~FD_READ;
#endif

	do
	{
		result = recv(sock->descriptor, buf, len, flags);
#if defined(WIN32)
		if (result < 0)
		{
			TranslateSocketError();
#if !defined(FRONTEND)
			if (errno == EWOULDBLOCK &&
				sock->blocking &&
				pg_socket_wait(sock, FD_READ))
				continue;
#endif
		}
#endif
	} while (false);

	return result;
}

/*
 * Return the raw kernel descriptor/handle.
 */
pg_socket_descriptor_t
pg_socket_descriptor(Socket *sock)
{
	return sock->descriptor;
}

/*
 * Put the socket in to blocking or non-blocking mode.
 */
int
pg_socket_set_blocking(Socket *sock, bool blocking)
{
#ifdef WIN32
	/*
	 * On Windows, the socket is always non-blocking, but we simulate
	 * blocking.
	 */
	sock->blocking = blocking;
#else
	int			flags;

	flags = fcntl(sock->descriptor, F_GETFL);
	if (flags < 0)
		return -1;
	if (blocking)
		flags &= ~O_NONBLOCK;
	else
		flags |= O_NONBLOCK;
	if (fcntl(sock->descriptor, F_SETFL, flags) == -1)
		return -1;
#endif

	return 0;
}
