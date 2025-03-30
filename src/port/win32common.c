/*-------------------------------------------------------------------------
 *
 * win32common.c
 *	  Common routines shared among the win32*.c ports.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/win32common.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "port/pg_iovec.h"

/*
 * pgwin32_get_file_type
 *
 * Convenience wrapper for GetFileType() with specific error handling for all the
 * port implementations.  Returns the file type associated with a HANDLE.
 *
 * On error, sets errno with FILE_TYPE_UNKNOWN as file type.
 */
DWORD
pgwin32_get_file_type(HANDLE hFile)
{
	DWORD		fileType = FILE_TYPE_UNKNOWN;
	DWORD		lastError;

	errno = 0;

	/*
	 * When stdin, stdout, and stderr aren't associated with a stream the
	 * special value -2 is returned:
	 * https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/get-osfhandle
	 */
	if (hFile == INVALID_HANDLE_VALUE || hFile == (HANDLE) -2)
	{
		errno = EINVAL;
		return FILE_TYPE_UNKNOWN;
	}

	fileType = GetFileType(hFile);
	lastError = GetLastError();

	/*
	 * Invoke GetLastError in order to distinguish between a "valid" return of
	 * FILE_TYPE_UNKNOWN and its return due to a calling error.  In case of
	 * success, GetLastError() returns NO_ERROR.
	 */
	if (fileType == FILE_TYPE_UNKNOWN && lastError != NO_ERROR)
	{
		_dosmaperr(lastError);
		return FILE_TYPE_UNKNOWN;
	}

	return fileType;
}

/*
 * Windows scatter/gather works with lists of raw page addresses, which this
 * function produces from Unix iovec format.  All iovecs must be aligned to
 * PG_WIN32_FILE_SEGMENT_SIZE (but they always are in PostgreSQL when using
 * direct I/O, and vectored I/O is only available on Windows with direct I/O).
 * Returns zero on badly aligned or input that would exceed maxsegments.
 */
DWORD
pg_win32_iovec_to_file_segments(FILE_SEGMENT_ELEMENT * segments,
								int maxsegments,
								struct iovec *iov,
								int iovcnt)
{
	DWORD		nsegments = 0;

	for (int i = 0; i < iovcnt; ++i)
	{
		char	   *base = iov[i].iov_base;
		size_t		len = iov[i].iov_len;

		if (nsegments > maxsegments ||
			((intptr_t) base) % PG_WIN32_FILE_SEGMENT_SIZE != 0 ||
			len % PG_WIN32_FILE_SEGMENT_SIZE != 0)
			return 0;

		while (len > 0)
		{
			segments[nsegments++].Buffer = base;
			base += PG_WIN32_FILE_SEGMENT_SIZE;
			len -= PG_WIN32_FILE_SEGMENT_SIZE;
		}
	}

	return nsegments * PG_WIN32_FILE_SEGMENT_SIZE;
}
