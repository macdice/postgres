/*-------------------------------------------------------------------------
 *
 * read_stream_pool.h
 *	  Mechanism for managing a pool of ReadStream objects.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/read_stream_pool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef READ_STREAM_SET_H
#define READ_STREAM_SET_H

#include "storage/bufmgr.h"
#include "storage/smgr.h"

typedef struct ReadStreamPool ReadStreamPool;

typedef BlockNumber (*ReadStreamPoolBlockCB) (ReadStreamPool *set,
											  void *callback_private_data,
											  void *per_buffer_data,
											  SMgrRelation *smgr,
											  ForkNumber *forknum,
											  ReadBufferMode *mode);

extern ReadStreamPool *read_stream_pool_begin(int flags,
											  int max_streams,
											  BufferAccessStrategyType strategy_type,
											  int strategy_size,
											  ReadStreamPoolBlockCB callback,
											  void *callback_private_data,
											  size_t per_buffer_data_size);
extern void read_stream_pool_end(ReadStreamPool *pool);
extern Buffer read_stream_pool_next_buffer(ReadStreamPool *pool,
										   void **per_buffer_data);
extern void read_stream_pool_reset(ReadStreamPool *pool);
extern void read_stream_pool_set_max_streams(ReadStreamPool *pool,
											 int max_stream);

#endif
