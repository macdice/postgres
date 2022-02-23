#ifndef PG_SOCKET_H
#define PG_SOCKET_H

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

/* The native type for sockets on this platform. */
typedef pgsocket pg_socket_descriptor_t;

extern Socket *pg_socket_open(pg_socket_descriptor_t descriptor);

extern void pg_socket_close(Socket *sock);

extern ssize_t pg_socket_send(Socket *sock,
							  const void *buf,
							  size_t len,
							  int flags);

extern ssize_t pg_socket_recv(Socket *socket,
							  void *buf,
							  size_t len,
							  int flags);

extern pg_socket_descriptor_t pg_socket_descriptor(Socket *sock);

extern int pg_socket_set_blocking(Socket *sock, bool blocking);

#endif
