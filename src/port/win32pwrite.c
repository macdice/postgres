/*-------------------------------------------------------------------------
 *
 * win32pwrite.c
 *	  Implementation of pwrite(2) for Windows.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/win32pwrite.c
 *
 *-------------------------------------------------------------------------
 */


#include "c.h"
#include "port/pg_iovec.h"

#include <windows.h>

ssize_t
pg_pwrite(int fd, const void *buf, size_t size, off_t offset)
{
	OVERLAPPED	overlapped = {0};
	HANDLE		handle;
	DWORD		result;

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	/* Avoid overflowing DWORD. */
	size = Min(size, 1024 * 1024 * 1024);

	/* Note that this changes the file position, despite not using it. */
	overlapped.Offset = offset;
	if (!WriteFile(handle, buf, size, &result, &overlapped))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	return result;
}

/*
 * Special emulation of pwritev() that works with O_DIRECT | O_OVERLAPPED.
 * See pg_iovec.h for general emulation.
 */
ssize_t
pg_win32_direct_pwritev(int fd, struct iovec *iov, int iovcnt, off_t offset)
{
	FILE_SEGMENT_ELEMENT segments[PG_WIN32_FILE_SEGMENTS_MAX];
	OVERLAPPED	overlapped = {0};
	HANDLE		handle;
	DWORD		size;
	DWORD		result;

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	size = pg_win32_iovec_to_file_segments(segments, lengthof(segments),
										   iov, iovcnt);
	if (size == 0)
	{
		errno = EINVAL;
		return -1;
	}

	if (!WriteFileGather(handle, segments, size, &result, &overlapped))
	{
		if (GetLastError() == ERROR_HANDLE_EOF)
			return 0;

		if (GetLastError() == ERROR_IO_PENDING)
		{
			if (!GetOverlappedResult(overlapped.hEvent,
									 &overlapped,
									 &result,
									 TRUE))
			{
				if (GetLastError() == ERROR_HANDLE_EOF)
					return 0;

				_dosmaperr(GetLastError());
				return -1;
			}
		}
		else
		{
			_dosmaperr(GetLastError());
			return -1;
		}
	}

	return result;
}
