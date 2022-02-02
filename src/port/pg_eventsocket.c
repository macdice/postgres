#include "postgres.h"

#include <winsock2.h>

struct PGEventSocketData *
pg_eventsocket_open(SOCKET sock)
{
	struct PGEventSocketData *eventsock;

	eventsock = (struct PGEventSocketData *) malloc(sizeof(*eventsock));
	if (!eventsock)
	{
		errno = ENOMEM;
		return NULL;
	}
	if ((eventsock->event_handle = WSACreateEvent()) == WSA_INVALID_EVENT)
	{
		int errno_save;

		_dosmaperr(WSAGetLastError());
		errno_save = errno;
		free(eventsock);
		errno = errno_save;

		/* XXX callers must all check for this, not done yet */

		return NULL;
	}
	eventsock->sock = sock;
	eventsock->selected_flags = 0;
	eventsock->received_flags = 0;

	return eventsock;
}

void
pg_eventsocket_close(struct PGEventSocketData *eventsock)
{
	WSACloseEvent(eventsock->event_handle);
	closesocket(eventsock->sock);
	free(eventsock);
}

pgsocket
pg_eventsocket_socket(struct PGEventSocketData *eventsock)
{
	return eventsock->sock;
}
