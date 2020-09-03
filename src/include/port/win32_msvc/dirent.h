/*
 * Headers for port/dirent.c, win32 native implementation of dirent functions
 *
 * src/include/port/win32_msvc/dirent.h
 */

#ifndef _WIN32VC_DIRENT_H
#define _WIN32VC_DIRENT_H
struct dirent
{
	long		d_ino;
	unsigned short d_reclen;
	unsigned char d_type;
	unsigned short d_namlen;
	char		d_name[MAX_PATH];
};

typedef struct DIR DIR;

DIR		   *opendir(const char *);
struct dirent *readdir(DIR *);
int			closedir(DIR *);

/* File types for 'd_type'. */
enum
  {
	DT_UNKNOWN = 0,
# define DT_UNKNOWN		DT_UNKNOWN
	DT_FIFO = 1,
# define DT_FIFO		DT_FIFO
	DT_CHR = 2,
# define DT_CHR			DT_CHR
	DT_DIR = 4,
# define DT_DIR			DT_DIR
	DT_BLK = 6,
# define DT_BLK			DT_BLK
	DT_REG = 8,
# define DT_REG			DT_REG
	DT_LNK = 10,
# define DT_LNK			DT_LNK
	DT_SOCK = 12,
# define DT_SOCK		DT_SOCK
	DT_WHT = 14
# define DT_WHT			DT_WHT
  };
#endif
