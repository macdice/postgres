/*-------------------------------------------------------------------------
 *
 * io_worker.h
 *    IO worker for implementing AIO "ourselves"
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/io.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IO_WORKER_H
#define IO_WORKER_H


pg_noreturn extern void IoWorkerMain(const void *startup_data, size_t startup_data_len);

extern PGDLLIMPORT int io_min_workers;
extern PGDLLIMPORT int io_max_workers;
extern PGDLLIMPORT int io_worker_idle_timeout;
extern PGDLLIMPORT int io_worker_launch_interval;

/* Interfaces visible to the postmaster. */
extern bool pgaio_worker_test_new_worker_needed(void);
extern bool pgaio_worker_clear_new_worker_needed(void);

#endif							/* IO_WORKER_H */
