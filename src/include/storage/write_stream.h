/*-------------------------------------------------------------------------
 *
 * write_stream.h
 *	  Mechanism for writing out buffered data efficiently
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/write_stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WRITE_STREAM_H
#define WRITE_STREAM_H

#include "storage/bufmgr.h"

/*
 * An opaque handle type returned by write_stream_write_buffer.  Calls can
 * optionally wait for individual buffers to be written using this handle.
 */
typedef struct WriteStreamWriteHandle
{
	uint64	   *p;
	uint64		c;
} WriteStreamWriteHandle;

struct WriteStream;
typedef struct WriteStream WriteStream;

extern WriteStream *write_stream_begin(int flags,
									   struct WritebackContext *wb_context,
									   int max_deferred_writes);
extern WriteStreamWriteHandle write_stream_write_buffer(WriteStream *stream,
														Buffer buffer);
extern bool write_stream_wait_internal(WriteStream *stream,
									   WriteStreamWriteHandle handle,
									   bool no_stall);
extern void write_stream_wait_all(WriteStream *stream);
extern void write_stream_reset(WriteStream *stream);
extern void write_stream_end(WriteStream *stream);

/*
 * Test if a write handle is valid.  Returns false for zero-initialized
 * handles.
 */
static inline bool
write_stream_handle_is_valid(WriteStreamWriteHandle handle)
{
	return handle.p;
}

/*
 * Test if a write is finished.
 */
static inline bool
write_stream_poll_handle(WriteStreamWriteHandle handle)
{
	/*
	 * If the completion counter has moved, then we know the containing write
	 * is not in progress.  This scheme allows handles to remain valid, while
	 * underlying objects are recycled.  The counter is 64 bit to avoid ABA
	 * problems.
	 */
	return write_stream_handle_is_valid(handle) && *handle.p != handle.c;
}

/*
 * Wait for a write to complete.  Since we don't have real asynchronous I/O,
 * this really means executing a synchronous write, or determining that it's
 * already been done.  The write is completed on return.
 *
 * Returns true if we had to stall, which currently means that we had to flush
 * the WAL or wait for someone else to flush the WAL.  (In future asynchronous
 * code it would have a more literal definition.)  Returns false if no waiting
 * was required.
 */
static inline bool
write_stream_wait(WriteStream *stream, WriteStreamWriteHandle handle)
{
	bool		stalled;

	if (!write_stream_handle_is_valid(handle) ||
		write_stream_poll_handle(handle))
		return false;

	stalled = write_stream_wait_internal(stream, handle, false);

	return stalled;
}

/*
 * For callers who don't want to stall, but want to check if a write is
 * already completed or can be completed without stalling.  See
 * write_stream_wait() for definition of stalling.
 *
 * Returns true if we would have had to stall, in which case the write might
 * not yet finished and the caller should wait again.  This is intended for
 * callers who have the option to increase their buffer of deferred writes to
 * reduce stalls.  Returns false if the write is completed already.
 */
static inline bool
write_stream_wait_no_stall(WriteStream *stream, WriteStreamWriteHandle handle)
{
	bool		would_stall;

	if (!write_stream_handle_is_valid(handle) ||
		write_stream_poll_handle(handle))
		return false;

	would_stall = write_stream_wait_internal(stream, handle, true);

	return would_stall;
}

#endif
