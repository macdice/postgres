#ifndef PG_STREAM_H
#define PG_STREAM_H

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

/*
 * This is the native type for sockets on the platform, but we'll give it a
 * different name than 'pgsocket' because it also works for pipes.
 */
typedef pgsocket pg_stream_descriptor_t;

/*
 * Note: The type pg_stream is defined in postgres_ext.h so that a libpq API
 * can use it.
 */

extern pg_stream *pg_stream_open(pg_stream_descriptor_t descriptor);

extern void pg_stream_close(pg_stream *stream);

extern ssize_t pg_stream_send(pg_stream *stream,
							  const void *buf,
							  size_t len,
							  int flags);

extern ssize_t pg_stream_recv(pg_stream *stream,
							  void *buf,
							  size_t len,
							  int flags);

extern pg_stream_descriptor_t pg_stream_descriptor(pg_stream *stream);

extern int pg_stream_set_blocking(pg_stream *stream, bool blocking);

#endif
