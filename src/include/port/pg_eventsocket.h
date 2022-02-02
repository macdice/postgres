#ifndef PG_EVENTSOCKET_H
#define PG_EVENTSOCKET_H

#include <sys/socket.h>
#include <sys/types.h>

static inline ssize_t
pg_eventsocket_send(PGEventSocket eventsock, const void *buf, size_t len,
					int flags)
{
#ifdef WIN32
	/*
	 * send() is one of the functions that re-enables the FD_WRITE event, so
	 * we can clear it from our set of 'sticky' events used for edge->level
	 * conversion.
	 *
	 * https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsaeventselect
	 */
	eventsock->received_flags &= ~FD_WRITE;
#endif
	return send(pg_eventsocket_socket(eventsock), buf, len, flags);
}

static inline ssize_t
pg_eventsocket_recv(PGEventSocket eventsock, void *buf, size_t len, int flags)
{
#ifdef WIN32
	/*
	 * recv() is one of the functions that re-enables the FD_READ event, so we
	 * can clear it from our set of 'sticky' events used for edge->level
	 * conversion.
	 *
	 * https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-wsaeventselect
	 */
	eventsock->received_flags &= ~FD_READ;
#endif
	return recv(pg_eventsocket_socket(eventsock), buf, len, flags);
}

#endif
