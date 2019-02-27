/*-------------------------------------------------------------------------
 *
 * segment.h
 *	  POSTGRES disk segment definitions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/segment.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEGMENT_H
#define SEGMENT_H


/*
 * Segment Number:
 *
 * Each relation and its forks are divided into segments. This
 * definition formalizes the definition of the segment number.
 */
typedef uint32 SegmentNumber;

#define InvalidSegmentNumber ((SegmentNumber) 0xFFFFFFFF)

#endif							/* SEGMENT_H */
