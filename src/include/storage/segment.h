/*-------------------------------------------------------------------------
 *
 * segment.h
 *   POSTGRES disk segment definitions.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/segment.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEGMENT_H
#define SEGMENT_H

#include "storage/block.h"

/*
 * We avoid creating very large disk files by cutting relations up into
 * smaller segment files.  Since there are values of RELSEG_SIZE and BLCKSZ
 * that would require md.c to create more than 2^16 segments for a relation
 * with MaxBlockNumber blocks, we can't use anything smaller than the size
 * we use for BlockNumber, so just define one in terms of the other.
 */
typedef BlockNumber SegmentNumber;

#endif							/* SEGMENT_H */
