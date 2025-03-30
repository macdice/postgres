/*-------------------------------------------------------------------------
 *
 * win32pread.c
 *	  Implementation of pread(2) for Windows.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/win32pread.c
 *
 *-------------------------------------------------------------------------
 */


#include "c.h"
#include "port/pg_iovec.h"

#include <windows.h>

ssize_t
pg_pread(int fd, void *buf, size_t size, off_t offset)
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
	if (!ReadFile(handle, buf, size, &result, &overlapped))
	{
		if (GetLastError() == ERROR_HANDLE_EOF)
			return 0;

		/*
		 * If the caller opened with O_OVERLAPPED but then called this
		 * explicitly synchronous function, we might need to wait for
		 * completion.
		 */
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

/*
 * Special emulation of preadv() that works with O_DIRECT | O_OVERLAPPED.
 * See pg_iovec.h for general emulation.
 */
ssize_t
pg_win32_direct_preadv(int fd, struct iovec *iov, int iovcnt, off_t offset)
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

	if (!ReadFileScatter(handle, segments, size, &result, &overlapped))
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
