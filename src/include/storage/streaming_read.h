#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"

struct PgStreamingRead;
typedef struct PgStreamingRead PgStreamingRead;

typedef bool (*PgStreamingReadBufferDetermineNextCB)(PgStreamingRead *pgsr,
													 uintptr_t pgsr_private,
													 BufferManagerRelation *bmr,
													 ForkNumber *forkNum,
													 BlockNumber *blockNum,
													 ReadBufferMode *mode);

extern PgStreamingRead *pg_streaming_read_buffer_alloc(int max_ios,
													   uintptr_t pgsr_private,
													   BufferAccessStrategy strategy,
													   PgStreamingReadBufferDetermineNextCB determine_next_cb);
extern void pg_streaming_read_prefetch(PgStreamingRead *pgsr);
extern uintptr_t pg_streaming_read_get_next(PgStreamingRead *pgsr);
extern void pg_streaming_read_reset(PgStreamingRead *pgsr);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);
extern int pg_streaming_read_inflight(PgStreamingRead *pgsr);
extern int pg_streaming_read_completed(PgStreamingRead *pgsr);

#endif
