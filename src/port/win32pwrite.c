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

#ifndef FRONTEND
static HANDLE
completion_event(void)
{
	static HANDLE h = NULL;

	if (!h && !(h = CreateEvent(NULL, true, false, NULL)))
		errno = ENOMEM;
	return h;
}
#endif

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

	/*
	 * Note that this changes the file position despite not using it unless
	 * opened with O_OVERLAPPED.
	 */
	overlapped.Offset = offset;

#ifndef FRONTEND
	/* If not synchronously written to kernel cache, we may need to wait. */
	if (unlikely(!(overlapped.hEvent = completion_event())))
		return -1;
#endif

	if (!WriteFile(handle, buf, size, &result, &overlapped))
	{
#ifndef FRONTEND
		if (GetLastError() == ERROR_IO_PENDING &&
			GetOverlappedResult(overlapped.hEvent,
								&overlapped,
								&result,
								TRUE))
			return result;
#endif

		_dosmaperr(GetLastError());
		return -1;
	}

	return result;
}

#ifndef FRONTEND
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
		if (GetLastError() == ERROR_IO_PENDING &&
			GetOverlappedResult(overlapped.hEvent,
								&overlapped,
								&result,
								TRUE))
			return result;

		_dosmaperr(GetLastError());
		return -1;
	}

	return result;
}
#endif
