#include <sys/socket.h>

/*
 * Windows has gai_strerrorA(), but it is not thread-safe.
 *
 * These are the error values documented by:
 *
 * https://learn.microsoft.com/en-us/windows/win32/api/ws2tcpip/nf-ws2tcpip-getaddrinfo
 */
const char *
gai_strerror(int errcode)
{
	switch (errcode)
	{
		case EAI_AGAIN:
			return "Temporary failure in name resolution";
		case EAI_BADFLAGS:
			return "Bad value for ai_flags";
		case EAI_FAIL:
			return "Non-recoverable failure in name resolution";
		case EAI_FAMILY:
			return "ai_family not supported";
		case EAI_MEMORY:
			return "Memory allocation failure";
		case EAI_NONAME:
			return "Name or service not known";
		case EAI_SERVICE:
			return "Servname not supported for ai_socktype";
		case EAI_SOCKTYPE:
			return "ai_socktype not supported";
		default:
			return "Unknown server error";
	}
}
