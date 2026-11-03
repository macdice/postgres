/*-------------------------------------------------------------------------
 *
 * read_stream_pool.c
 *	  Mechanism for managing a pool of ReadStream objects.
 *
 * The interface is similar to ReadStream, but the user-supplied callback can
 * read from any fork of any SMgrRelation.
 *
 * This is intended for use by recovery, where the work of potentially many
 * concurrent sessions is replayed.  The goal is to recreate a similar degree
 * of I/O combining and read-ahead at redo time.
 *
 * Portions Copyright (c) 2024-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/read_stream_pool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "common/hashfn.h"
#include "port/pg_bitutils.h"
#include "storage/read_stream.h"
#include "storage/read_stream_pool.h"

/* Initial sizes of internal queues, which later grow on demand. */
#define READ_STREAM_POOL_STREAM_QUEUE_SIZE 64
#define READ_STREAM_POOL_STREAM_BLOCK_QUEUE_SIZE 16

typedef struct ReadStreamPoolMemberKey
{
	SMgrRelation smgr;
	ForkNumber	forknum;
} ReadStreamPoolMemberKey;

typedef struct ReadStreamPoolMember
{
	ReadStreamPoolMemberKey key;

	ReadStreamPool *pool;
	ReadStream *stream;
	BufferAccessStrategy strategy;

	bool		in_draining_queue;
	dlist_node	node;

	int			block_queue_head;
	int			block_queue_tail;
	int			block_queue_mask;
	BlockNumber *block_queue;
} ReadStreamPoolMember;

typedef struct ReadStreamPoolMemberEntry
{
	ReadStreamPoolMemberKey key;
	int			status;
	ReadStreamPoolMember *member;
} ReadStreamPoolMemberEntry;

#define SH_PREFIX read_stream_pool_table
#define SH_ELEMENT_TYPE ReadStreamPoolMemberEntry
#define SH_KEY_TYPE ReadStreamPoolMemberKey
#define SH_KEY key
#define SH_EQUAL(table, a, b) ((a).smgr == (b).smgr && (a).forknum == (b).forknum)
#define SH_HASH_KEY(table, key) murmurhash64(((uintptr_t) (key).smgr) ^ (key).forknum)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

typedef struct ReadStreamPool
{
	/* Parameters. */
	int			max_streams;
	int			flags;
	BufferAccessStrategyType strategy_type;
	int			strategy_size;
	ReadStreamPoolBlockCB callback;
	void	   *callback_private_data;
	size_t		per_buffer_data_size;
	MemoryContext context;

	/* Queue recording the sequence of underlying streams to consume from. */
	int			stream_queue_head;
	int			stream_queue_tail;
	int			stream_queue_mask;
	ReadStream **stream_queue;

	/* Corresponding per-buffer data, if per_buffer_data_size > 0. */
	char	   *stream_queue_data;

	/* Members. */
	dclist_head member_lru_queue;
	dclist_head member_draining_queue;
	read_stream_pool_table_hash *member_table;
	ReadStreamPoolMember *paused;
} ReadStreamPool;

static BlockNumber
			read_stream_pool_next_block(ReadStream *stream,
										void *callback_private_data,
										void *per_buffer_data);

static void
read_stream_pool_grow_block_queue(ReadStreamPool *pool,
								  ReadStreamPoolMember *member)
{
	size_t		old_size = member->block_queue_mask + 1;
	size_t		new_size = old_size * 2;

	member->block_queue = repalloc(member->block_queue, new_size);

	if (member->block_queue_head < member->block_queue_tail)
	{
		if (member->block_queue_head > 0)
			memcpy(member->block_queue + old_size,
				   member->block_queue,
				   sizeof(member->block_queue[0] * member->block_queue_head));
		member->block_queue_head += old_size;
	}

	member->block_queue_mask = new_size - 1;
}

static void
read_stream_pool_enqueue_block(ReadStreamPool *pool,
							   ReadStreamPoolMember *member,
							   BlockNumber blocknum)
{
	int			head = member->block_queue_head;
	int			new_head = (head + 1) & member->block_queue_mask;

	if (unlikely(new_head == member->block_queue_tail))
	{
		read_stream_pool_grow_block_queue(pool, member);
		head = member->block_queue_head;
		new_head = (head + 1) & member->block_queue_mask;
	}

	member->block_queue[head] = blocknum;
	member->block_queue_head = new_head;
}

static bool
read_stream_pool_can_dequeue_block(ReadStreamPoolMember *member)
{
	return member->block_queue_head != member->block_queue_tail;
}

static BlockNumber
read_stream_pool_dequeue_block(ReadStreamPoolMember *member)
{
	BlockNumber blocknum;

	Assert(read_stream_pool_can_dequeue_block(member));

	blocknum = member->block_queue[member->block_queue_tail];
	member->block_queue_tail++;
	member->block_queue_tail &= member->block_queue_mask;
	return blocknum;
}

static void
read_stream_pool_enqueue_switch(ReadStreamPool *pool,
								ReadStreamPoolMember *member,
								SMgrRelation smgr,
								ForkNumber forknum)
{
	uintptr_t	smgr_value = (uintptr_t) smgr;

	read_stream_pool_enqueue_block(pool, member, InvalidBlockNumber);
#if SIZEOF_VOID_P > 4
	read_stream_pool_enqueue_block(pool, member, smgr_value >> 32);
#endif
	read_stream_pool_enqueue_block(pool, member, smgr_value);
	read_stream_pool_enqueue_block(pool, member, forknum);
}

static void
read_stream_pool_dequeue_switch(ReadStreamPoolMember *member,
								SMgrRelation *smgr,
								ForkNumber *forknum)
{
	uintptr_t	smgr_value;

	/* Caller has already consumed leading InvalidBlockNumber. */
	smgr_value = read_stream_pool_dequeue_block(member);
#if SIZEOF_VOID_P > 4
	smgr_value <<= 32;
	smgr_value |= read_stream_pool_dequeue_block(member);
#endif
	*smgr = (SMgrRelation) smgr_value;
	*forknum = read_stream_pool_dequeue_block(member);
}

static ReadStreamPoolMember *
read_stream_pool_open_member(ReadStreamPool *pool,
							 ReadStreamPoolMemberKey key)
{
	ReadStreamPoolMemberEntry *entry;
	ReadStreamPoolMember *member;
	bool		found;

	if (dclist_count(&pool->member_lru_queue) +
		dclist_count(&pool->member_draining_queue) < pool->max_streams)
	{
		MemoryContext old_context = MemoryContextSwitchTo(pool->context);

		/* Allocate a new stream. */
		member = palloc_object(ReadStreamPoolMember);
		member->key = key;
		member->pool = pool;
		member->in_draining_queue = false;
		member->block_queue_head = 0;
		member->block_queue_tail = 0;
		member->block_queue_mask = READ_STREAM_POOL_STREAM_BLOCK_QUEUE_SIZE - 1;
		member->block_queue =
			palloc_array(BlockNumber, READ_STREAM_POOL_STREAM_BLOCK_QUEUE_SIZE);

		if (pool->strategy_size <= 0)
			member->strategy = GetAccessStrategy(pool->strategy_type);
		else
			member->strategy = GetAccessStrategyWithSize(pool->strategy_type,
														 pool->strategy_size);

		member->stream = read_stream_begin_smgr_relation(pool->flags,
														 member->strategy,
														 key.smgr,
														 RELPERSISTENCE_PERMANENT,
														 key.forknum,
														 read_stream_pool_next_block,
														 member,
														 pool->per_buffer_data_size);

		dclist_push_head(&pool->member_lru_queue, &member->node);

		entry = read_stream_pool_table_insert(pool->member_table, member->key,
											  &found);
		Assert(!found);
		entry->member = member;

		MemoryContextSwitchTo(old_context);
	}
	else
	{
		/*
		 * Tell the least recently used stream to switch to this relation/fork
		 * at this point in its block number queue.
		 */
		member = dclist_container(ReadStreamPoolMember, node,
								  dclist_tail_node(&pool->member_lru_queue));

		read_stream_pool_enqueue_switch(pool, member, key.smgr, key.forknum);
		dclist_move_head(&pool->member_lru_queue, &member->node);

		read_stream_pool_table_delete(pool->member_table, member->key);

		member->key = key;
		entry = read_stream_pool_table_insert(pool->member_table, member->key,
											  &found);
		Assert(!found);
		entry->member = member;
	}
	return member;
}

static bool
read_stream_pool_member_is_referenced(ReadStreamPool *pool,
									  ReadStreamPoolMember *member)
{
	for (int i = pool->stream_queue_tail;
		 i != pool->stream_queue_head;
		 i = (i + 1) % pool->stream_queue_mask)
		if (pool->stream_queue[i] == member->stream)
			return true;
	return false;
}

static void
read_stream_pool_free_member(ReadStreamPool *pool,
							 ReadStreamPoolMember *member)
{
	Assert(!read_stream_pool_member_is_referenced(pool, member));

	if (member->in_draining_queue)
	{
		dclist_delete_from(&pool->member_draining_queue, &member->node);
	}
	else
	{
		dclist_delete_from(&pool->member_lru_queue, &member->node);
		read_stream_pool_table_delete(pool->member_table, member->key);
	}
	FreeAccessStrategy(member->strategy);
	pfree(member->block_queue);
	pfree(member);
}

static bool
read_stream_pool_try_free_member(ReadStreamPool *pool,
								 ReadStreamPoolMember *member)
{
	Assert(member->in_draining_queue);

	if (!read_stream_pool_member_is_referenced(pool, member))
	{
		read_stream_pool_free_member(pool, member);
		return true;
	}
	return false;
}

static void
read_stream_pool_close_member(ReadStreamPool *pool, ReadStreamPoolMember *member)
{
	/* Stop it from looking further ahead. */
	read_stream_pause(member->stream);

	/* Stop putting new blocks into its queue. */
	read_stream_pool_table_delete(pool->member_table, member->key);
	dclist_delete_from(&pool->member_lru_queue, &member->node);

	/*
	 * We might have to wait until all references to member->stream are
	 * drained from stream_queue.  We'll try now, and then again each time a
	 * buffer is pulled from it.
	 */
	member->in_draining_queue = true;
	dclist_push_head(&pool->member_draining_queue, &member->node);
	read_stream_pool_try_free_member(pool, member);
}

static ReadStreamPoolMember *
read_stream_pool_get_member(ReadStreamPool *pool,
							SMgrRelation smgr,
							ForkNumber forknum)
{
	ReadStreamPoolMemberKey key = {smgr, forknum};
	ReadStreamPoolMemberEntry *entry;

	entry = read_stream_pool_table_lookup(pool->member_table, key);
	if (likely(entry))
	{
		ReadStreamPoolMember *member = entry->member;

		dclist_move_head(&pool->member_lru_queue, &member->node);
		return member;
	}

	return read_stream_pool_open_member(pool, key);
}

static void
read_stream_pool_grow_stream_queue(ReadStreamPool *pool)
{
	size_t		old_size = pool->stream_queue_mask + 1;
	size_t		new_size = old_size * 2;

	pool->stream_queue = repalloc(pool->stream_queue, new_size);
	if (pool->stream_queue_data)
		pool->stream_queue_data = repalloc(pool->stream_queue_data,
										   pool->per_buffer_data_size *
										   new_size);

	if (pool->stream_queue_head < pool->stream_queue_tail)
	{
		if (pool->stream_queue_head > 0)
		{
			memcpy(pool->stream_queue + old_size,
				   pool->stream_queue,
				   sizeof(pool->stream_queue[0]) * pool->stream_queue_head);
			if (pool->stream_queue_data)
				memcpy(pool->stream_queue_data +
					   pool->per_buffer_data_size * old_size,
					   pool->stream_queue_data,
					   pool->per_buffer_data_size * pool->stream_queue_head);
		}
		pool->stream_queue_head += old_size;
	}

	pool->stream_queue_mask = new_size - 1;
}

static void
read_stream_pool_enqueue_stream(ReadStreamPool *pool, ReadStream *stream)
{
	/*
	 * There is always space for one more.  This ensures that the registered
	 * callback can write into pool->stream_queue_data before we enqueue the
	 * stream here, and their positions are aligned.
	 */
	Assert(pool->stream_queue_head != pool->stream_queue_tail);

	pool->stream_queue[pool->stream_queue_head++] = stream;
	pool->stream_queue_head &= pool->stream_queue_mask;

	/* Ensure that there is one more free entry in the queue. */
	if (((pool->stream_queue_head + 1) & pool->stream_queue_mask) ==
		pool->stream_queue_tail)
		read_stream_pool_grow_stream_queue(pool);
}

static void *
read_stream_pool_per_buffer_data(ReadStreamPool *pool, int index)
{
	return pool->stream_queue_data + pool->per_buffer_data_size * index;
}

/*
 * Consume one relation/forknum/blocknum from the registered callback and
 * assign a stream to handle it.
 */
static void
read_stream_pool_pump(ReadStreamPool *pool)
{
	ReadStreamPoolMember *member;
	SMgrRelation smgr;
	ForkNumber	forknum;
	BlockNumber blocknum;
	ReadBufferMode mode;
	void	   *per_buffer_data;

	per_buffer_data =
		read_stream_pool_per_buffer_data(pool, pool->stream_queue_head);

	blocknum = pool->callback(pool,
							  pool->callback_private_data,
							  per_buffer_data,
							  &smgr,
							  &forknum,
							  &mode);

	member = read_stream_pool_get_member(pool, smgr, forknum);
	read_stream_pool_enqueue_block(pool, member, blocknum);
	read_stream_pool_enqueue_stream(pool, member->stream);
}

/*
 * Callback provided to member ReadStreams.
 */
static BlockNumber
read_stream_pool_next_block(ReadStream *stream,
							void *callback_private_data,
							void *per_buffer_data)
{
	ReadStreamPoolMember *member = (ReadStreamPoolMember *) callback_private_data;
	ReadStreamPool *pool = member->pool;
	SMgrRelation smgr;
	ForkNumber	forknum;
	BlockNumber blocknum;

	/* ReadBufferMode mode; */

	do
	{
		/* Return block numbers already queued for this stream. */
		while (read_stream_pool_can_dequeue_block(member))
		{
			blocknum = read_stream_pool_dequeue_block(member);

			if (likely(blocknum != InvalidBlockNumber))
				return blocknum;

			read_stream_pool_dequeue_switch(member, &smgr, &forknum);
			read_stream_switch_smgr_relation(member->stream,
											 smgr,
											 RELPERSISTENCE_PERMANENT,
											 forknum);
		}

		read_stream_pool_pump(pool);
	} while (!pool->paused);

	return InvalidBlockNumber;
}

ReadStreamPool *
read_stream_pool_begin(int flags,
					   int max_streams,
					   BufferAccessStrategyType strategy_type,
					   int strategy_size,
					   ReadStreamPoolBlockCB callback,
					   void *callback_private_data,
					   size_t per_buffer_data_size)
{
	ReadStreamPool *pool = palloc_object(ReadStreamPool);

	pool->context = CurrentMemoryContext;
	pool->max_streams = max_streams;
	pool->strategy_type = strategy_type;
	pool->strategy_size = strategy_size;
	pool->callback = callback;
	pool->callback_private_data = callback_private_data;
	pool->per_buffer_data_size = per_buffer_data_size;
	pool->context = CurrentMemoryContext;

	pool->stream_queue_head = 0;
	pool->stream_queue_tail = 0;
	pool->stream_queue_mask = READ_STREAM_POOL_STREAM_QUEUE_SIZE - 1;
	pool->stream_queue = palloc_array(ReadStream *,
									  READ_STREAM_POOL_STREAM_QUEUE_SIZE);
	pool->stream_queue_data = NULL;
	if (per_buffer_data_size > 0)
		pool->stream_queue_data = palloc(READ_STREAM_POOL_STREAM_QUEUE_SIZE *
										 per_buffer_data_size);
	else
		pool->stream_queue_data = NULL;

	dclist_init(&pool->member_lru_queue);
	dclist_init(&pool->member_draining_queue);

	pool->member_table = read_stream_pool_table_create(pool->context,
													   max_streams,
													   NULL);

	return pool;
}

void
read_stream_pool_end(ReadStreamPool *pool)
{
	pool->stream_queue_tail = pool->stream_queue_head;

	for (dclist_head *q = &pool->member_lru_queue; !dclist_is_empty(q);)
		read_stream_pool_close_member(pool,
									  dclist_container(ReadStreamPoolMember,
													   node,
													   dclist_pop_head_node(q)));

	read_stream_pool_table_destroy(pool->member_table);
	pfree(pool->stream_queue);
	if (pool->stream_queue_data)
		pfree(pool->stream_queue_data);
	pfree(pool);
}

static bool
read_stream_pool_has_draining_member(ReadStreamPool *pool)
{
	return dclist_count(&pool->member_draining_queue) > 0;
}

static void
read_stream_pool_drain(ReadStreamPool *pool)
{
	ReadStreamPoolMember *member;
	dclist_head *q = &pool->member_draining_queue;

	Assert(read_stream_pool_has_draining_member(pool));

	/*
	 * Free draining members from the tail of the queue until we hit one that
	 * still has a reference in stream_queue.
	 */
	do
		member = dclist_container(ReadStreamPoolMember,
								  node,
								  dclist_tail_node(q));
	while (read_stream_pool_try_free_member(pool, member) &&
		   read_stream_pool_has_draining_member(pool));
}

Buffer
read_stream_pool_next_buffer(ReadStreamPool *pool, void **per_buffer_data)
{
	ReadStream *stream;
	Buffer		buffer;

	if (pool->stream_queue_tail == pool->stream_queue_head)
		read_stream_pool_pump(pool);

	if (pool->stream_queue_tail == pool->stream_queue_head)
		return InvalidBuffer;

	stream = pool->stream_queue[pool->stream_queue_tail];
	if (per_buffer_data)
		*per_buffer_data = read_stream_pool_per_buffer_data(pool,
															pool->stream_queue_tail);

	pool->stream_queue_tail++;
	pool->stream_queue_tail &= pool->stream_queue_mask;

	buffer = read_stream_next_buffer(stream, NULL);

	if (unlikely(read_stream_pool_has_draining_member(pool)))
		read_stream_pool_drain(pool);

	return buffer;
}

void
read_stream_pool_reset(ReadStreamPool *pool)
{
	ReadStreamPoolMember *member;
	dlist_iter	iter;

	pool->stream_queue_tail = pool->stream_queue_head;

	dclist_foreach(iter, &pool->member_lru_queue)
	{
		member = dlist_container(ReadStreamPoolMember, node, iter.cur);
		read_stream_reset(member->stream);
	}
}

/*
 * Modify the maximum number of streams.  If set to a lower number, the least
 * recently used streams will be closed.  They are not actually freed until
 * all buffers are consumed from them.
 */
void
read_stream_pool_set_max_streams(ReadStreamPool *pool, int max_stream)
{
	ReadStreamPoolMember *member;

	pool->max_streams = max_stream;

	while (dclist_count(&pool->member_lru_queue) > max_stream)
	{
		member = dclist_container(ReadStreamPoolMember, node,
								  dclist_tail_node(&pool->member_lru_queue));
		read_stream_pool_close_member(pool, member);
	}
}

#if 0
static void
read_stream_pool_close_smgr(ReadStreamPool *pool, SMgrRelation smgr)
{
	for (ForkNumber forknum = 0; forknum <= MAX_FORKNUM; ++forknum)
	{
		ReadStreamPoolMemberKey key = {smgr, forknum};
		ReadStreamPoolMemberEntry *entry =
			read_stream_pool_table_lookup(pool->member_table, key);

		if (entry)
			read_stream_pool_close_member(pool, entry->member);
	}
}
#endif
