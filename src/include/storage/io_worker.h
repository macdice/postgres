/*-------------------------------------------------------------------------
 *
 * io_worker.h
 *    IO worker for implementing AIO "ourselves"
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/io_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IO_WORKER_H
#define IO_WORKER_H


pg_noreturn extern void IoWorkerMain(const void *startup_data, size_t startup_data_len);

/* Public GUCs. */
extern PGDLLIMPORT int io_min_workers;
extern PGDLLIMPORT int io_max_workers;
extern PGDLLIMPORT int io_worker_idle_timeout;
extern PGDLLIMPORT int io_worker_launch_interval;

/* Developer-only GUCs accessed with "debug_" prefixes. */
extern PGDLLIMPORT int io_worker_queue_size;
extern PGDLLIMPORT int io_worker_limit_iops;
extern PGDLLIMPORT int io_worker_limit_read;
extern PGDLLIMPORT int io_worker_limit_write;
extern PGDLLIMPORT bool io_worker_overflow_sync;

extern void assign_debug_io_worker_limit_iops(int newval, void *extra);
extern void assign_debug_io_worker_limit_read(int newval, void *extra);
extern void assign_debug_io_worker_limit_write(int newval, void *extra);
extern const char *show_debug_io_worker_limit_iops(void);
extern const char *show_debug_io_worker_limit_read(void);
extern const char *show_debug_io_worker_limit_write(void);

/* Interfaces visible to the postmaster. */
extern bool pgaio_worker_test_grow(void);
extern bool pgaio_worker_test_and_clear_grow(void);

#endif							/* IO_WORKER_H */
