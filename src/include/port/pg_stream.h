#ifndef PG_STREAM_H
#define PG_STREAM_H

#include "postgres.h"

/*
 * This is the native type for sockets on the platform, but we'll give it a
 * different name than 'pgsocket' because it also works for pipes.
 */
typedef pgsocket pg_stream_descriptor_t;

struct pg_stream;
typedef struct pg_stream pg_stream;

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
