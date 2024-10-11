#ifndef AUTO_READ_STREAM_H
#define AUTO_READ_STREAM_H

#include "storage/bufmgr.h"

struct AutoReadStream;
typedef struct AutoReadStream AutoReadStream;

extern AutoReadStream *auto_read_stream_begin(BufferAccessStrategy strategy,
											  Relation rel,
											  ForkNumber forknum);
extern void auto_read_stream_end(AutoReadStream *auto_stream);
extern Buffer auto_read_buffer(AutoReadStream *auto_stream,
							   BlockNumber blocknum);

#endif
