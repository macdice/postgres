/*
 * A routine to report the name and pid of a process that has a file open.
 * Based on example code from Raymond Chen's blog:
 *
 * https://devblogs.microsoft.com/oldnewthing/20120217-00/?p=8283
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <windows.h>
#include <RestartManager.h>

/*
 * Try to report the PID of an arbitrary process that currently has a given
 * file open, if there is one.
 */
size_t
win32_file_opened_by(const char *path, int *pids, size_t npids)
{
	DWORD dwSession;
	WCHAR szSessionKey[CCH_RM_SESSION_KEY + 1] = {0};
	DWORD dwError;
	size_t result = 0;

	dwError = RmStartSession(&dwSession, 0, szSessionKey);
	if (dwError == ERROR_SUCCESS)
	{
		wchar_t wpath[MAX_PATH];
		wchar_t *wpaths = wpath;

		if (mbstowcs(wpath, path, MAX_PATH) == MAX_PATH)
			wpath[MAX_PATH - 1] = 0;

		dwError = RmRegisterResources(dwSession, 1, &wpaths, 0, NULL, 0, NULL);
		if (dwError == ERROR_SUCCESS)
		{
			DWORD dwReason;
			UINT nProcInfoNeeded;
			UINT nProcInfo = npids;
			RM_PROCESS_INFO *rgpi;

			rgpi = palloc(sizeof(RM_PROCESS_INFO) * npids);
			dwError = RmGetList(dwSession, &nProcInfoNeeded,
								&nProcInfo, rpgi, &dwReason);
			if (dwError == ERROR_SUCCESS)
			{
				for (size_t i = 0; i < nProcInfo; ++i)
					pids[i] = rgpi[i].Process.dwProcessId;
				result = nProcInfo;
			}
			pfree(rgpi);
		}
		RmEndSession(dwSession);
	}

	return result;
}
