/*-------------------------------------------------------------------------
 *
 * shm_mmq.h
 *	  multi-reader, single-writer shared memory message queue
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shm_mmq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHM_MMQ_H
#define SHM_MMQ_H

struct shm_mmq;
typedef struct shm_mmq shm_mmq;

struct shm_mmq_handle;
typedef struct shm_mmq_handle shm_mmq_handle;

extern shm_mmq_handle *shm_mmq_init(void *address, int pages);
extern shm_mmq_handle *shm_mmq_attach(void *address);
extern size_t shm_mmq_estimate(int pages);

#endif							/* SHM_MMQ_H */
