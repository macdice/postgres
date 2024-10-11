#include "postgres.h"
#include "storage/auto_read_stream.h"
#include "storage/bufmgr.h"
#include "storage/read_stream.h"

struct AutoReadStream
{
	ReadStream *stream;
	Relation	rel;
	BlockNumber next;
	BlockNumber last;
};

static BlockNumber
auto_read_stream_cb(ReadStream *stream,
					void *callback_private_data,
					void *per_buffer_data)
{
	AutoReadStream *auto_stream = callback_private_data;
	BlockNumber next = auto_stream->next;

	if (next == InvalidBlockNumber)
		return InvalidBuffer;
	auto_stream->next = Min(auto_stream->next + 1, auto_stream->last);

	return next;
}

AutoReadStream *
auto_read_stream_begin(BufferAccessStrategy strategy,
					   Relation rel,
					   ForkNumber forknum)
{
	AutoReadStream *auto_stream;

	auto_stream = palloc(sizeof(*auto_stream));
	auto_stream->rel = rel;
	auto_stream->next = InvalidBlockNumber;
	auto_stream->last = InvalidBlockNumber;
	auto_stream->stream = read_stream_begin_relation(0,
													 strategy,
													 rel,
													 forknum,
													 auto_read_stream_cb,
													 auto_stream,
													 0);

	return auto_stream;
}

void
auto_read_stream_end(AutoReadStream *auto_stream)
{
	read_stream_end(auto_stream->stream);
	pfree(auto_stream);
}

Buffer
auto_read_buffer(AutoReadStream *auto_stream, BlockNumber blocknum)
{

	if (auto_stream->last != InvalidBlockNumber)
	{
		Buffer		buffer = read_stream_next_buffer(auto_stream->stream, NULL);

		/* Did the stream guess right? */
		if (buffer != InvalidBlockNumber)
		{
			if (BufferGetBlockNumber(buffer) == blocknum)
				return buffer;
			ReleaseBuffer(buffer);
		}
	}

	/* Figure out the highest block number if we haven't already. */
	if (auto_stream->last == InvalidBlockNumber)
	{
		BlockNumber nblocks;

		nblocks = RelationGetNumberOfBlocks(auto_stream->rel);
		if (nblocks > 0)
			auto_stream->last = nblocks - 1;
	}

	/* Take a guess at the next block. */
	if (auto_stream->last != InvalidBlockNumber &&
		auto_stream->last >= blocknum + 1)
		auto_stream->next = blocknum + 1;
	read_stream_reset(auto_stream->stream);

	return ReadBuffer(auto_stream->rel, blocknum);
}
