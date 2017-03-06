/*-------------------------------------------------------------------------
 *
 * sharedtuplestore.h
 *	  Simple mechinism for sharing tuples between backends.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/sharedtuplestore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHAREDTUPLESTORE_H
#define SHAREDTUPLESTORE_H

struct SharedTuplestore;
typedef struct SharedTuplestore SharedTuplestore;

struct SharedTuplestoreAccessor;
typedef struct SharedTuplestoreAccessor SharedTuplestoreAccessor;

#define SHARED_TUPLESTORE_SINGLE_PASS 0x01

extern Size sts_size(int participants);

extern SharedTuplestoreAccessor *sts_initialize(SharedTuplestore *sts,
												int participants,
												int my_participant_number,
												Size meta_data_size,
												int flags,
												dsm_segment *segment);

extern SharedTuplestoreAccessor *sts_attach(SharedTuplestore *sts,
											int my_participant_number,
											dsm_segment *segment);

extern void sts_end_write(SharedTuplestoreAccessor *accessor,
						  int partition);

extern void sts_prepare_parallel_read(SharedTuplestoreAccessor *accessor,
									  int partition);

extern void sts_begin_parallel_read(SharedTuplestoreAccessor *accessor,
									int partition);

extern void sts_puttuple(SharedTuplestoreAccessor *accessor,
						 int partition,
						 void *meta_data,
						 MinimalTuple tuple);


extern MinimalTuple sts_gettuple(SharedTuplestoreAccessor *accessor,
								 void *meta_data);

#endif   /* SHAREDTUPLESTORE_H */
