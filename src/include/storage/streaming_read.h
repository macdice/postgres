#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"

/*
 * For most sequential access, callers can user this size to build full sized
 * reads without pinning many extra buffers.
 */
#define PG_STREAMING_READ_DEFAULT_MAX_IOS MAX_BUFFERS_PER_TRANSFER

struct PgStreamingRead;
typedef struct PgStreamingRead PgStreamingRead;

/* "Simple" callback that returns a block number. */
typedef BlockNumber (*PgStreamingReadBufferCB) (PgStreamingRead *pgsr,
												uintptr_t pgsr_private,
												void *per_io_private);

/* Callback allowing the relation, fork, block and mode to vary. */
typedef bool (*PgStreamingReadBufferExtendedCB) (PgStreamingRead *pgsr,
												 uintptr_t pgsr_private,
												 void *per_io_private,
												 BufferManagerRelation *bmr,
												 ForkNumber *forknum,
												 BlockNumber *blockNum,
												 ReadBufferMode *mode);

extern PgStreamingRead *pg_streaming_read_buffer_alloc(int max_ios,
													   size_t per_io_private_size,
													   uintptr_t pgsr_private,
													   BufferAccessStrategy strategy,
													   BufferManagerRelation bmr,
													   ForkNumber forknum,
													   PgStreamingReadBufferCB next_block_cb);

extern PgStreamingRead *pg_streaming_read_buffer_alloc_ext(int max_ios,
														   size_t per_io_private_size,
														   uintptr_t pgsr_private,
														   BufferAccessStrategy strategy,
														   PgStreamingReadBufferExtendedCB next_block_cb);

extern void pg_streaming_read_prefetch(PgStreamingRead *pgsr);
extern Buffer pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_io_private);
extern void pg_streaming_read_reset(PgStreamingRead *pgsr);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);

extern int pg_streaming_read_ios(PgStreamingRead *pgsr);
extern int pg_streaming_read_pins(PgStreamingRead *pgsr);

#endif
