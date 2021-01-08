/* src/include/port/freebsd.h */

/*
 * Set the default wal_sync_method to fdatasync, because xlogdefs.h's rules
 * would otherwise prefer open_datasync on FreeBSD 13+.  That isn't a good
 * default for all workloads and file systems.
 */
#ifdef HAVE_FDATASYNC
#define PLATFORM_DEFAULT_SYNC_METHOD	SYNC_METHOD_FDATASYNC
#endif
