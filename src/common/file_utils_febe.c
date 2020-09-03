/*-------------------------------------------------------------------------
 *
 * File-processing utility routines.
 *
 * Assorted utility functions to work on files, frontend and backend.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/common/file_utils_febe.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include <dirent.h>
#include <sys/stat.h>

#include "common/file_utils.h"
#ifdef FRONTEND
#include "common/logging.h"
#else
#include "utils/elog.h"
#endif

/*
 * Return the type of a directory entry.
 *
 * In frontend code, elevel should be a level from logging.h; in backend code
 * it should be an error level from elog.h.
 */
PGFileType
get_dirent_type(const char *path,
				const struct dirent *de,
				bool look_through_symlinks,
				int elevel)
{
	PGFileType	result;

	/*
	 * We want to know the type of a directory entry.  Some systems tell us
	 * that directly in the dirent struct, but that's a BSD/GNU extension.
	 * Even when the interface is present, sometimes the type is unknown,
	 * depending on the filesystem in use or in some cases options used at
	 * filesystem creation time.
	 */
#if defined(DT_UNKNOWN) && defined(DT_REG) && defined(DT_DIR) && defined(DT_LNK)
	if (de->d_type == DT_REG)
		result = PGFILETYPE_REG;
	else if (de->d_type == DT_DIR)
		result = PGFILETYPE_DIR;
	else if (de->d_type == DT_LNK && !look_through_symlinks)
		result = PGFILETYPE_LNK;
	else
		result = PGFILETYPE_UNKNOWN;
#else
	result = PGFILETYPE_UNKNOWN;
#endif

	if (result == PGFILETYPE_UNKNOWN)
	{
		struct stat fst;
		int			sret;


		if (look_through_symlinks)
			sret = stat(path, &fst);
		else
			sret = lstat(path, &fst);

		if (sret < 0)
		{
			result = PGFILETYPE_ERROR;
#ifdef FRONTEND
			pg_log_generic(elevel, "could not stat file \"%s\": %m", path);
#else
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", path)));
#endif
		}
		else if (S_ISREG(fst.st_mode))
			result = PGFILETYPE_REG;
		else if (S_ISDIR(fst.st_mode))
			result = PGFILETYPE_DIR;
#ifdef S_ISLNK
		else if (S_ISLNK(fst.st_mode))
			result = PGFILETYPE_LNK;
#endif
	}

	return result;
}
