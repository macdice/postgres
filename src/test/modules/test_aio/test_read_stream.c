/*-------------------------------------------------------------------------
 *
 * test_read_stream.c
 *		Helpers to write tests for read_stream.c
 *
 * Copyright (c) 2020-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_aio/test_read_stream.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/read_stream.h"

typedef struct
{
	BlockNumber blkno;
	int			count;
} test_read_stream_resume_state;

static BlockNumber
test_read_stream_resume_cb(ReadStream *stream,
						   void *callback_private_data,
						   void *per_buffer_data)
{
	test_read_stream_resume_state *state = callback_private_data;

	/* Periodic end-of-stream. */
	if (++state->count % 3 == 0)
		return read_stream_pause(stream);

	return state->blkno;
}

/*
 * Test read_stream_resume(), allowing a stream to end temporarily and then
 * continue where it left off.
 */
PG_FUNCTION_INFO_V1(test_read_stream_resume);
Datum
test_read_stream_resume(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	BlockNumber blkno = PG_GETARG_UINT32(1);
	Relation	rel;
	Buffer		buf;
	ReadStream *stream;
	test_read_stream_resume_state state = {.blkno = blkno};

	rel = relation_open(relid, AccessShareLock);
	stream = read_stream_begin_relation(READ_STREAM_DEFAULT,
										NULL,
										rel,
										MAIN_FORKNUM,
										test_read_stream_resume_cb,
										&state,
										0);

	for (int i = 0; i < 3; ++i)
	{
		/* Same block twice. */
		buf = read_stream_next_buffer(stream, NULL);
		Assert(BufferGetBlockNumber(buf) == blkno);
		ReleaseBuffer(buf);
		buf = read_stream_next_buffer(stream, NULL);
		Assert(BufferGetBlockNumber(buf) == blkno);
		ReleaseBuffer(buf);

		/* End-of-stream. */
		buf = read_stream_next_buffer(stream, NULL);
		Assert(buf == InvalidBuffer);
		buf = read_stream_next_buffer(stream, NULL);
		Assert(buf == InvalidBuffer);

		/* Resume. */
		read_stream_resume(stream);
	}

	read_stream_end(stream);
	relation_close(rel, NoLock);

	PG_RETURN_VOID();
}

typedef struct
{
	BlockNumber blkno;
	int			count;
	int			yields;
	int			blocks;
}			test_read_stream_yield_state;

static BlockNumber
test_read_stream_yield_cb(ReadStream *stream,
						  void *callback_private_data,
						  void *per_buffer_data)
{
	test_read_stream_yield_state *state = callback_private_data;

	/* Yield every third call. */
	if (++state->count % 3 == 2)
	{
		state->yields++;
		return read_stream_yield(stream);
	}

	state->blocks++;
	return state->blkno;
}

/*
 * Test read_stream_yield(), allowing control to be yielded temporarily from
 * the lookahead loop and returned to the caller of read_stream_next_buffer().
 */
PG_FUNCTION_INFO_V1(test_read_stream_yield);
Datum
test_read_stream_yield(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	BlockNumber blkno = PG_GETARG_UINT32(1);
	Relation	rel;
	Buffer		buf;
	ReadStream *stream;
	test_read_stream_yield_state state = {.blkno = blkno};

	rel = relation_open(relid, AccessShareLock);
	stream = read_stream_begin_relation(READ_STREAM_DEFAULT,
										NULL,
										rel,
										MAIN_FORKNUM,
										test_read_stream_yield_cb,
										&state,
										0);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 1);
	Assert(state.yields == 1);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 3);
	Assert(state.yields == 1);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 3);
	Assert(state.yields == 2);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 5);
	Assert(state.yields == 2);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 5);
	Assert(state.yields == 3);

	buf = read_stream_next_buffer(stream, NULL);
	Assert(BufferGetBlockNumber(buf) == blkno);
	ReleaseBuffer(buf);
	Assert(state.blocks == 7);
	Assert(state.yields == 3);

	read_stream_end(stream);
	relation_close(rel, NoLock);

	PG_RETURN_VOID();
}
