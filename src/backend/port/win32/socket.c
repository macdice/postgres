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

#include "common/hashfn.h"

/*
 * Indicate if pgwin32_recv() and pgwin32_send() should operate
 * in non-blocking mode.
 *
 * Since the socket emulation layer always sets the actual socket to
 * non-blocking mode in order to be able to deliver signals, we must
 * specify this in a separate flag if we actually need non-blocking
 * operation.
 *
 * This flag changes the behaviour *globally* for all socket operations,
 * so it should only be set for very short periods of time.
 */
int			pgwin32_noblock = 0;

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
 * An entry in our socket table.
 */
typedef struct SocketTableEntry
{
	SOCKET		sock;
	char		status;

	/*
	 * The reference count for the event handle.  Client code that wants to
	 * use the event functions must acquire a reference and release it when
	 * finished.
	 */
	int			reference_count;

	/*
	 * The FD_XXX events that were most recently selected for this socket
	 * number with WSAEventSelect().
	 */
	int			selected_events;

	/*
	 * The FD_XXX events already reported by Winsock, that we'll continue to
	 * report as long as they are true.  They are cleared by our send/recv
	 * wrappers, because those are 're-enabling' functions that will cause
	 * Winsock to report them again.  The are also cleared by an explicit
	 * check we perform for the benefit of hypothetical code that might be
	 * reach Winsock send/recv wrappers without going via our wrappers.
	 */
	int			level_triggered_events;

	/*
	 * Windows kernel event most recently associated with the socket number.
	 */
	HANDLE		event_handle;
} SocketTableEntry;

static inline void *
malloc0(size_t size)
{
	void	   *result;

	result = malloc(size);
	if (result)
		memset(result, 0, size);

	return result;
}

/*
 * It almost seems feasible to use an array to store our per-socket state,
 * based on the observation that Windows socket descriptors seem to be small
 * integers as on Unix, but the manual warns against making that assumption.
 * So we use a hash table.
 */

#define SH_PREFIX socket_table
#define SH_ELEMENT_TYPE SocketTableEntry
#define SH_RAW_ALLOCATOR malloc0
#define SH_RAW_FREE free
#define SH_SCOPE static inline
#define SH_KEY_TYPE SOCKET
#define SH_KEY sock
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_EQUAL(tb, a, b) (a) == (b)
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static socket_table_hash * socket_table;

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

static int
isDataGram(SOCKET s)
{
	int			type;
	int			typelen = sizeof(type);

	if (getsockopt(s, SOL_SOCKET, SO_TYPE, (char *) &type, &typelen))
		return 1;

	return (type == SOCK_DGRAM) ? 1 : 0;
}

int
pgwin32_waitforsinglesocket(SOCKET s, int what, int timeout)
{
	static HANDLE waitevent = INVALID_HANDLE_VALUE;
	static SOCKET current_socket = INVALID_SOCKET;
	static int	isUDP = 0;
	HANDLE		events[2];
	int			r;

	/* Create an event object just once and use it on all future calls */
	if (waitevent == INVALID_HANDLE_VALUE)
	{
		waitevent = CreateEvent(NULL, TRUE, FALSE, NULL);

		if (waitevent == INVALID_HANDLE_VALUE)
			ereport(ERROR,
					(errmsg_internal("could not create socket waiting event: error code %lu", GetLastError())));
	}
	else if (!ResetEvent(waitevent))
		ereport(ERROR,
				(errmsg_internal("could not reset socket waiting event: error code %lu", GetLastError())));

	/*
	 * Track whether socket is UDP or not.  (NB: most likely, this is both
	 * useless and wrong; there is no reason to think that the behavior of
	 * WSAEventSelect is different for TCP and UDP.)
	 */
	if (current_socket != s)
		isUDP = isDataGram(s);
	current_socket = s;

	/*
	 * Attach event to socket.  NOTE: we must detach it again before
	 * returning, since other bits of code may try to attach other events to
	 * the socket.
	 */
	if (WSAEventSelect(s, waitevent, what) != 0)
	{
		TranslateSocketError();
		return 0;
	}

	events[0] = pgwin32_signal_event;
	events[1] = waitevent;

	/*
	 * Just a workaround of unknown locking problem with writing in UDP socket
	 * under high load: Client's pgsql backend sleeps infinitely in
	 * WaitForMultipleObjectsEx, pgstat process sleeps in pgwin32_select().
	 * So, we will wait with small timeout(0.1 sec) and if socket is still
	 * blocked, try WSASend (see comments in pgwin32_select) and wait again.
	 */
	if ((what & FD_WRITE) && isUDP)
	{
		for (;;)
		{
			r = WaitForMultipleObjectsEx(2, events, FALSE, 100, TRUE);

			if (r == WAIT_TIMEOUT)
			{
				char		c;
				WSABUF		buf;
				DWORD		sent;

				buf.buf = &c;
				buf.len = 0;

				r = WSASend(s, &buf, 1, &sent, 0, NULL, NULL);
				if (r == 0)		/* Completed - means things are fine! */
				{
					WSAEventSelect(s, NULL, 0);
					return 1;
				}
				else if (WSAGetLastError() != WSAEWOULDBLOCK)
				{
					TranslateSocketError();
					WSAEventSelect(s, NULL, 0);
					return 0;
				}
			}
			else
				break;
		}
	}
	else
		r = WaitForMultipleObjectsEx(2, events, FALSE, timeout, TRUE);

	WSAEventSelect(s, NULL, 0);

	if (r == WAIT_OBJECT_0 || r == WAIT_IO_COMPLETION)
	{
		pgwin32_dispatch_queued_signals();
		errno = EINTR;
		return 0;
	}
	if (r == WAIT_OBJECT_0 + 1)
		return 1;
	if (r == WAIT_TIMEOUT)
	{
		errno = EWOULDBLOCK;
		return 0;
	}
	ereport(ERROR,
			(errmsg_internal("unrecognized return value from WaitForMultipleObjects: %d (error code %lu)", r, GetLastError())));
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

	s = WSASocket(af, type, protocol, NULL, 0, WSA_FLAG_OVERLAPPED);
	if (s == INVALID_SOCKET)
	{
		TranslateSocketError();
		return INVALID_SOCKET;
	}

	if (ioctlsocket(s, FIONBIO, &on))
	{
		TranslateSocketError();
		return INVALID_SOCKET;
	}
	errno = 0;

	return s;
}

/*
 * Check if any of FD_READ, FD_WRITE or FD_CLOSE is still true.  Used to
 * re-check level-triggered events.
 */
static int
pgwin32_socket_poll(SOCKET s, int events)
{
	int			revents = 0;

	if (events & (FD_READ | FD_CLOSE))
	{
		ssize_t		rc;
		char		c;

		rc = recv(s, &c, 1, MSG_PEEK);
		if (rc == 1)
		{
			/* At least one byte to read. */
			if (events & FD_READ)
				revents |= FD_READ;
		}
		else if (rc == 0 || WSAGetLastError() != WSAEWOULDBLOCK)
		{
			/* EOF due to graceful shutdown, or error. */
			if (events & FD_CLOSE)
				revents |= FD_CLOSE;
		}
	}

	if (events & FD_WRITE)
	{
		char		c;

		/* If it looks like we could write or get an error, report that. */
		if (send(s, &c, 0, 0) == 0 || WSAGetLastError() != WSAEWOULDBLOCK)
			revents |= FD_WRITE;
	}

	return revents;
}

/*
 * Adjust the set of FD_XXX events this socket's event handle should wake up
 * for.  Returns 0 on success, otherwise -1 and sets errno.
 */
int
pgwin32_socket_select_events(SOCKET s, int selected_events)
{
	SocketTableEntry *entry;

	Assert(socket_table);
	entry = socket_table_lookup(socket_table, s);

	Assert(entry);
	Assert(entry->reference_count > 0);
	Assert(entry->event_handle != WSA_INVALID_EVENT);

	/* Do nothing if no change. */
	if (selected_events == entry->selected_events)
		return 0;

	/*
	 * Tell Winsock to link the socket to the event handle, and which events
	 * we're interested in.
	 */
	if (WSAEventSelect(s, entry->event_handle, selected_events) == SOCKET_ERROR)
	{
		TranslateSocketError();
		return -1;
	}

	entry->selected_events = selected_events;

	/*
	 * The manual tells us: "Issuing a WSAEventSelect for a socket cancels any
	 * previous WSAAsyncSelect or WSAEventSelect for the same socket and
	 * clears the internal network event record."  If that is true, we might
	 * have wiped an internal flag we're interested in.  Close that race by
	 * triggering an explicit poll before we sleep, by pretending we have seen
	 * all of these events.
	 */
	if (selected_events & (FD_READ | FD_WRITE))
		entry->level_triggered_events = selected_events & (FD_READ | FD_WRITE | FD_CLOSE);
	else
		entry->level_triggered_events = 0;

	return 0;
}

/*
 * Before waiting on the event handle, check if we have pending
 * level-triggered events that are still true, and if so take measures to
 * prevent the sleep.
 */
void
pgwin32_socket_prepare_to_wait(SOCKET s)
{
	SocketTableEntry *entry;

	Assert(socket_table);
	entry = socket_table_lookup(socket_table, s);

	Assert(entry);
	Assert(entry->reference_count > 0);
	Assert(entry->event_handle != WSA_INVALID_EVENT);

	/*
	 * If we're not waiting for FD_READ or FD_WRITE, don't try to poll the
	 * socket.  Server sockets and client sockets that haven't connected yet
	 * can't be polled by that technique.
	 */
	if ((entry->selected_events & (FD_READ | FD_WRITE)) &&
		entry->level_triggered_events != 0)
	{
		/*
		 * Re-check the level-triggered events we have recorded.  This is
		 * necessary because someone might access WSASend()/WSARecv() directly
		 * without going via our wrapper functions, so they might never be
		 * cleared otherwise.
		 */
		entry->level_triggered_events =
			pgwin32_socket_poll(s,
								entry->level_triggered_events & entry->selected_events);
		if (entry->level_triggered_events)
		{
			/*
			 * At least one readiness condition is still true.  Prevent
			 * sleeping, and let pgwin32_socket_enumerate_events() report
			 * these level-triggered events.
			 */
			WSASetEvent(entry->event_handle);
		}
	}
}

/*
 * After the Windows event handle has been signaled, this function can be
 * called to find out which socket events occurred, and atomically reset the
 * event handle for the next sleep.
 *
 * The events returned are also remembered in our level-triggered event mask,
 * so they'll prevent sleeping and be reported again as long as they remain
 * true.
 */
int
pgwin32_socket_enumerate_events(SOCKET s)
{
	WSANETWORKEVENTS new_events = {0};
	SocketTableEntry *entry;
	int			result;

	Assert(socket_table);
	entry = socket_table_lookup(socket_table, s);

	Assert(entry);
	Assert(entry->reference_count > 0);
	Assert(entry->event_handle != WSA_INVALID_EVENT);

	/*
	 * Atomically consume the internal network event record and reset the
	 * associated event handle.  This guarantees that we can't miss future
	 * wakeups.
	 */
	if (WSAEnumNetworkEvents(s, entry->event_handle, &new_events) != 0)
	{
		TranslateSocketError();
		return -1;
	}

	/* Add any events pgwin32_socket_prepare_to_wait() decided to feed us. */
	result = entry->level_triggered_events | new_events.lNetworkEvents;

	/* Remember certain events for next time around. */
	if (entry->selected_events & (FD_READ | FD_WRITE))
		entry->level_triggered_events = result & (FD_READ | FD_WRITE | FD_CLOSE);
	else
		entry->level_triggered_events = 0;

	return result;
}

/*
 * Acquire a reference-counted Windows event handle for this socket.  This can
 * be used for waiting for socket events.  Returns NULL and sets errno on
 * failure.
 */
HANDLE
pgwin32_socket_acquire_event_handle(SOCKET s)
{
	SocketTableEntry *entry;
	bool		found;

	/* First-time initialization. */
	if (unlikely(socket_table == NULL))
	{
		socket_table = socket_table_create(16, NULL);
		if (socket_table == NULL)
		{
			errno = ENOMEM;
			return NULL;
		}
	}

	/* If we already have it, just bump the count. */
	entry = socket_table_insert(socket_table, s, &found);
	if (likely(found))
	{
		Assert(entry->event_handle != WSA_INVALID_EVENT);
		entry->reference_count++;
		return entry->event_handle;
	}

	/* Did we run out of memory? */
	if (entry == NULL)
	{
		errno = ENOMEM;
		return NULL;
	}

	/* Allocate a new event handle. */
	entry->event_handle = WSACreateEvent();
	if (entry->event_handle == WSA_INVALID_EVENT)
	{
		socket_table_delete_item(socket_table, entry);
		errno = ENOMEM;
		return NULL;
	}

	entry->selected_events = 0;
	entry->level_triggered_events = 0;
	entry->reference_count = 1;

	return entry->event_handle;
}

/*
 * Release a reference-counted event handle.
 */
void
pgwin32_socket_release_event_handle(SOCKET s)
{
	SocketTableEntry *entry;

	Assert(socket_table);
	entry = socket_table_lookup(socket_table, s);

	Assert(entry);
	Assert(entry->reference_count > 0);
	Assert(entry->event_handle != WSA_INVALID_EVENT);

	if (--entry->reference_count == 0)
	{
		WSACloseEvent(entry->event_handle);
		socket_table_delete_item(socket_table, entry);

		/* XXX Free socket_table if it is empty? */
	}
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

	if (WSAGetLastError() != WSAEWOULDBLOCK)
	{
		TranslateSocketError();
		return -1;
	}

	while (pgwin32_waitforsinglesocket(s, FD_CONNECT, INFINITE) == 0)
	{
		/* Loop endlessly as long as we are just delivering signals */
	}

	return 0;
}

int
pgwin32_recv(SOCKET s, char *buf, int len, int f)
{
	WSABUF		wbuf;
	int			r;
	DWORD		b;
	DWORD		flags = f;
	int			n;

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
	 * WSARecv() is a re-enabling function for Winsock's FD_READ event, so it
	 * is now safe to clear our level-triggered flag.  This is only an
	 * optimization for a common case, and not required for correctness.  If
	 * someone calls WSARecv() directly instead of going through this wrapper,
	 * pgwin32_socket_prepare_to_wait() will figure that out and clear it
	 * anyway.
	 */
	if (socket_table)
	{
		SocketTableEntry *entry = socket_table_lookup(socket_table, s);

		if (entry)
			entry->level_triggered_events &= ~FD_READ;
	}

	if (pgwin32_noblock)
	{
		/*
		 * No data received, and we are in "emulated non-blocking mode", so
		 * return indicating that we'd block if we were to continue.
		 */
		errno = EWOULDBLOCK;
		return -1;
	}

	/* We're in blocking mode, so wait for data */

	for (n = 0; n < 5; n++)
	{
		if (pgwin32_waitforsinglesocket(s, FD_READ | FD_CLOSE | FD_ACCEPT,
										INFINITE) == 0)
			return -1;			/* errno already set */

		r = WSARecv(s, &wbuf, 1, &b, &flags, NULL, NULL);
		if (r != SOCKET_ERROR)
			return b;			/* success */
		if (WSAGetLastError() != WSAEWOULDBLOCK)
		{
			TranslateSocketError();
			return -1;
		}

		/*
		 * There seem to be cases on win2k (at least) where WSARecv can return
		 * WSAEWOULDBLOCK even when pgwin32_waitforsinglesocket claims the
		 * socket is readable.  In this case, just sleep for a moment and try
		 * again.  We try up to 5 times - if it fails more than that it's not
		 * likely to ever come back.
		 */
		pg_usleep(10000);
	}
	ereport(NOTICE,
			(errmsg_internal("could not read from ready socket (after retries)")));
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
		 * WSASend() is a re-enabling function for Winsock's FD_WRITE event,
		 * so it is now safe to clear our level-triggered flag.  This is only
		 * an optimization for a common case, and not required for
		 * correctness.  If someone calls WSASend() directly instead of going
		 * through this wrapper, pgwin32_socket_prepare_to_wait() will figure
		 * that out and clear it anyway.
		 */
		if (socket_table)
		{
			SocketTableEntry *entry = socket_table_lookup(socket_table, s);

			if (entry)
				entry->level_triggered_events &= ~FD_WRITE;
		}

		if (pgwin32_noblock)
		{
			/*
			 * No data sent, and we are in "emulated non-blocking mode", so
			 * return indicating that we'd block if we were to continue.
			 */
			errno = EWOULDBLOCK;
			return -1;
		}

		/* No error, zero bytes */

		if (pgwin32_waitforsinglesocket(s, FD_WRITE | FD_CLOSE, INFINITE) == 0)
			return -1;
	}

	return -1;
}
