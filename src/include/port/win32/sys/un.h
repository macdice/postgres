/*
 * src/include/port/win32/sys/un.h
 */
#ifndef WIN32_SYS_UN_H
#define WIN32_SYS_UN_H

/*
 * Windows headers don't define this structure, but you can define it yourself
 * to use the functionality.
 */
struct sockaddr_un
{
	unsigned short sun_family;
	char		sun_path[108];
};

#endif
