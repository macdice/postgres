/*-------------------------------------------------------------------------
 *
 * pqbuffer.h
 *	  Types and routines for managing queues of network buffers.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/pqbuffer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PQBUFFER_H
#define PQBUFFER_H

#include "lib/ilist.h"

/*
 * A socket buffer used for sending and receiving data.
 *
 */
typedef struct PqBuffer
{
	dlist_node	node;			/* Link for PortIoChannel queues. */
	uint8	   *data;			/* Pointer to I/O aligned memory. */
	uint32		begin;			/* Beginning of populated data. */
	uint32		end;			/* End of populated data. */
	uint32		max_end;		/* Maximum possible end of populated data. */

	/*
	 * A socket buffer can hold multiple 'segments' of populated bytes with
	 * holes in between.  For non-encrypted and TLS connections, there is just
	 * one segment covering the whole buffer.  For GSSAPI connections,
	 * cleartext buffers may have more than one segment, to allow space
	 * reserved for GSSAPI framing.  This complication allows GSSAPI to
	 * perform encryption and decryption in place, so that a large
	 * socket_buffer_size can contain many 16kB GSSAPI messages.
	 *
	 * If nsegments is > 1, then port_buffer_segment(buffer, n) can be used to
	 * select a different segment, which has the effect of changing the begin,
	 * end and max_end values.  The GSSAPI support code stores the state
	 * required to implement this in the spare holes, which exist only while a
	 * buffer holds cleartext.  When the buffer hold crypt text, it is always
	 * one segment of raw data to/from the network.
	 */
	int			nsegments;
	int			segment;
} PqBuffer;

/* An ordered queue of buffers. */
typedef dclist_head PqBufferQueue;

static inline void
bufq_init(PqBufferQueue *queue)
{
	dclist_init(queue);
}

static inline size_t
bufq_size(PqBufferQueue *queue)
{
	return dclist_count(queue);
}

static inline bool
bufq_empty(PqBufferQueue *queue)
{
	return dclist_is_empty(queue);
}

static inline PqBuffer *
bufq_head(PqBufferQueue *queue)
{
	return dclist_head_element(PqBuffer, node, queue);
}

static inline PqBuffer *
bufq_pop_head(PqBufferQueue *queue)
{
	PqBuffer   *buf;

	buf = dclist_head_element(PqBuffer, node, queue);
	dclist_pop_head_node(queue);
	return buf;
}

static inline void
bufq_push_head(PqBufferQueue *queue, PqBuffer *buf)
{
	dclist_push_head(queue, &buf->node);
}

static inline PqBuffer *
bufq_tail(PqBufferQueue *queue)
{
	return dclist_tail_element(PqBuffer, node, queue);
}

static inline void
bufq_push_tail(PqBufferQueue *queue, PqBuffer *buf)
{
	dclist_push_tail(queue, &buf->node);
}

static inline void
bufq_insert_after(PqBufferQueue *queue,
				  PqBuffer *insert_after,
				  PqBuffer *buf)
{
	dclist_insert_after(queue, &insert_after->node, &buf->node);
}

static inline bool
bufq_has_next(PqBufferQueue *queue, PqBuffer *buf)
{
	return dclist_has_next(queue, &buf->node);
}

static inline PqBuffer *
bufq_next(PqBufferQueue *queue, PqBuffer *buf)
{
	return dlist_container(PqBuffer, node, dclist_next_node(queue, &buf->node));
}

#endif
