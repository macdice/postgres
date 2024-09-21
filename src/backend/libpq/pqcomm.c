/*-------------------------------------------------------------------------
 *
 * pqcomm.c
 *	  Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data.
 *
 * To emit an outgoing message, use the routines in pqformat.c to construct
 * the message in a buffer and then emit it in one call to pq_putmessage.
 * There are no functions to send raw bytes or partial messages; this
 * ensures that the channel will not be clogged by an incomplete message if
 * execution is aborted by ereport(ERROR) partway through the message.
 *
 * At one time, libpq was shared between frontend and backend, but now
 * the backend's "backend/libpq" is quite separate from "interfaces/libpq".
 * All that remains is similarities of names to trap the unwary...
 *
 * Network I/O buffer architecture
 * ===============================
 *
 * A user controllable number of separate I/O-aligned buffers holds network
 * data in various stages of transfer to/from the network, configured by the
 * GUC "socket_buffers".  They are of configurable size "socket_buffer_size".
 * When not in use, they sit the queue port->free_buffers.
 *
 * The send and receive direction are called "channels".  Each channel has
 * three queues of buffers currently in use:
 *
 * 1. io_buffers: Buffers currently involved in an in-progress socket I/O
 *	  operation (AIO, or simulated AIO using synchronous readiness APIs).  The
 *	  buffers in this queue are also pointed to by channel->iov[], in the
 *	  format needed for scatter/gather kernel interfaces.
 *
 * 2. crypt_buffers: If using an encryption library, received buffers move
 *	  from io_buffers to this queue when an I/O completes, or from this queue
 *	  to io_buffers when an I/O begins.  If not using an encryption library,
 *	  this queue is not used.
 *
 * 3. clear_buffers:
 *
 * 3. 1. If not using an encryption library, recv buffers move from io_buffers
 *		 to clear_buffers when I/O completes, and send buffers move from
 *		 clear_buffers to io_buffers when I/O begins.
 *
 * 3. 2. If using an encyption library, recv data (not buffers) moves from
 *		 crypto_buffers to clear_buffers.  For OpenSSL we have to read/write
 *		 'through' the library from/to new buffers, and for GSSAPI we can
 *		 encrypt/decrypt directly in place and require whole buffers.
 *
 * 3. 3. XXX In future, if we added KTLS support for kernel software or
 *		 hardware-accelerated AES, then perhaps the OpenSSL data-copying
 *		 pathway (3.2) would only only be needed during session establishment,
 *		 and after that buffers could move directly between clear_buffers and
 *		 io_buffers (like 3.1)?
 *
 * When the backend sends data, it should try to append directly to the tail
 * buffer in port->send.clear_buffers, avoiding intermediate construction
 * buffers as much as possible.  When the backend receives data, it should try
 * to use pointers directly into the head buffer of port->recv.clear_buffers,
 * avoiding extra copying as much as possible.
 *
 * At the level of the io_buffers queues we use completion-based I/O
 * interfaces (possibly simulated with synchronous readiness APIs).  At the
 * level of the crypt_buffers queues we expose a readiness-style interface to
 * the encryption libraries, because that is what they currently expect.
 *
 * In order to be able to guarantee forward progress, XXX...
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/libpq/pqcomm.c
 *
 *-------------------------------------------------------------------------
 */

/*------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		ListenServerPort	- Open postmaster's server port
 *		AcceptConnection	- Accept new connection with client
 *		TouchSocketFiles	- Protect socket files against /tmp cleaners
 *		pq_init				- initialize libpq at backend startup
 *		socket_comm_reset	- reset libpq during error recovery
 *		socket_close		- shutdown libpq at backend exit
 *
 * low-level I/O:
 *		pq_getbytes		- get a known number of bytes from connection
 *		pq_getmessage	- get a message with length word from connection
 *		pq_getbyte		- get next byte from connection
 *		pq_peekbyte		- peek at next byte from connection
 *		pq_flush		- flush pending output
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *		pq_getbyte_if_available - get a byte if available without blocking
 *
 * message-level I/O
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *		pq_putmessage_noblock - buffer a normal message (suppressed in COPY OUT)
 *
 *------------------------
 */
#include "postgres.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include <signal.h>
#include <fcntl.h>
#include <grp.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <utime.h>
#ifdef WIN32
#include <mstcpip.h>
#endif

#include "common/ip.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "utils/guc_hooks.h"
#include "utils/memutils.h"
#include "utils/wait_event_types.h"

/*
 * Cope with the various platform-specific ways to spell TCP keepalive socket
 * options.  This doesn't cover Windows, which as usual does its own thing.
 */
#if defined(TCP_KEEPIDLE)
/* TCP_KEEPIDLE is the name of this option on Linux and *BSD */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPIDLE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPIDLE"
#elif defined(TCP_KEEPALIVE_THRESHOLD)
/* TCP_KEEPALIVE_THRESHOLD is the name of this option on Solaris >= 11 */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE_THRESHOLD
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE_THRESHOLD"
#elif defined(TCP_KEEPALIVE) && defined(__darwin__)
/* TCP_KEEPALIVE is the name of this option on macOS */
/* Caution: Solaris has this symbol but it means something different */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE"
#endif

/*
 * Configuration options
 */
int			Unix_socket_permissions;
char	   *Unix_socket_group;
int			socket_buffers = 32;
int			socket_buffer_size = 8192;
int			socket_combine_limit = 16;
bool		socket_aio = false;

/*
 * When performing bulk transfers, both the send and receive paths leave a
 * small number of buffers for the other direction.  We don't expect bulk
 * transfers in both directions at the same time, so we allow both directions
 * to use almost all the buffers when needed, while still leaving some for the
 * other direction.  It should be at least 3, to allow one cleartext buffer,
 * one encrypted buffer and one I/O in progress.
  */
#define SOCK_BUFS_FULL_DUPLEX_RESERVED 3

/* Where the Unix socket files are (list of palloc'd strings) */
static List *sock_paths = NIL;

/*
 * Message status
 */
static bool PqCommBusy;			/* busy sending data to the client */
static bool PqCommReadingMsg;	/* in the middle of reading a message */


/* Internal functions */
static void socket_comm_reset(void);
static void socket_close(int code, Datum arg);
static int	socket_flush(void);
static int	socket_putmessage(char msgtype, const char *s, size_t len);
static void socket_putmessage_noblock(char msgtype, const char *s, size_t len);

static int	Lock_AF_UNIX(const char *unixSocketDir, const char *unixSocketPath);
static int	Setup_AF_UNIX(const char *sock_path);

static const PQcommMethods PqCommSocketMethods = {
	.comm_reset = socket_comm_reset,
	.flush = socket_flush,
	.putmessage = socket_putmessage,
	.putmessage_noblock = socket_putmessage_noblock
};

const PQcommMethods *PqCommMethods = &PqCommSocketMethods;

WaitEventSet *FeBeWaitSet;


/* --------------------------------
 *		pq_init - initialize libpq at backend startup
 * --------------------------------
 */
Port *
pq_init(ClientSocket *client_sock)
{
	Port	   *port;
	int			socket_pos PG_USED_FOR_ASSERTS_ONLY;
	int			latch_pos PG_USED_FOR_ASSERTS_ONLY;
	PqBuffer   *bufs;
	uint8	   *buffer_space;

	/* allocate the Port struct and copy the ClientSocket contents to it */
	port = palloc0(sizeof(Port));
	port->sock = client_sock->sock;
	memcpy(&port->raddr.addr, &client_sock->raddr.addr, client_sock->raddr.salen);
	port->raddr.salen = client_sock->raddr.salen;

	/* fill in the server (local) address */
	port->laddr.salen = sizeof(port->laddr.addr);
	if (getsockname(port->sock,
					(struct sockaddr *) &port->laddr.addr,
					&port->laddr.salen) < 0)
	{
		ereport(FATAL,
				(errmsg("%s() failed: %m", "getsockname")));
	}

	/* select NODELAY and KEEPALIVE options if it's a TCP connection */
	if (port->laddr.addr.ss_family != AF_UNIX)
	{
		int			on;
#ifdef WIN32
		int			oldopt;
		int			optlen;
		int			newopt;
#endif

#ifdef	TCP_NODELAY
		on = 1;
		if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY,
					   (char *) &on, sizeof(on)) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_NODELAY")));
		}
#endif
		on = 1;
		if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE,
					   (char *) &on, sizeof(on)) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "setsockopt", "SO_KEEPALIVE")));
		}

#ifdef WIN32

		/*
		 * This is a Win32 socket optimization.  The OS send buffer should be
		 * large enough to send the whole Postgres send buffer in one go, or
		 * performance suffers.  The Postgres send buffer can be enlarged if a
		 * very large message needs to be sent, but we won't attempt to
		 * enlarge the OS buffer if that happens, so somewhat arbitrarily
		 * ensure that the OS buffer is at least big enough for our
		 * socket_combine_limit.
		 *
		 * We won't make it smaller than the default though, which is 64kB on
		 * current Windows version unless it has been adjusted through the
		 * registry.
		 */
		optlen = sizeof(oldopt);
		if (getsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &oldopt,
					   &optlen) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "getsockopt", "SO_SNDBUF")));
		}
		newopt = socket_buffer_size * socket_combine_limit;
		if (oldopt < newopt)
		{
			if (setsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &newopt,
						   sizeof(newopt)) < 0)
			{
				ereport(FATAL,
						(errmsg("%s(%s) failed: %m", "setsockopt", "SO_SNDBUF")));
			}
		}
#endif

		/*
		 * Also apply the current keepalive parameters.  If we fail to set a
		 * parameter, don't error out, because these aren't universally
		 * supported.  (Note: you might think we need to reset the GUC
		 * variables to 0 in such a case, but it's not necessary because the
		 * show hooks for these variables report the truth anyway.)
		 */
		(void) pq_setkeepalivesidle(tcp_keepalives_idle, port);
		(void) pq_setkeepalivesinterval(tcp_keepalives_interval, port);
		(void) pq_setkeepalivescount(tcp_keepalives_count, port);
		(void) pq_settcpusertimeout(tcp_user_timeout, port);
	}

	/* Allocate socket buffers, aligned on typical memory pages. */
	bufq_init(&port->recv.io_buffers);
	bufq_init(&port->recv.crypt_buffers);
	bufq_init(&port->recv.clear_buffers);
	port->recv.eof = false;
	port->recv.error = 0;
	bufq_init(&port->send.io_buffers);
	bufq_init(&port->send.crypt_buffers);
	bufq_init(&port->send.clear_buffers);
	bufq_init(&port->free_buffers);
	port->send.eof = false;
	port->send.error = 0;	
	buffer_space = MemoryContextAllocAligned(TopMemoryContext,
											 socket_buffers * socket_buffer_size,
											 PG_IO_ALIGN_SIZE,
											 0);
	bufs = MemoryContextAlloc(TopMemoryContext,
							  socket_buffers * sizeof(PqBuffer));
	for (int i = 0; i < socket_buffers; ++i)
	{
		PqBuffer   *buf = &bufs[i];

		buf->data = buffer_space + socket_buffer_size * i;
		bufq_push_tail(&port->free_buffers, buf);
	}

	/* initialize state variables */
	PqCommBusy = false;
	PqCommReadingMsg = false;

	/* set up process-exit hook to close the socket */
	on_proc_exit(socket_close, 0);

	/*
	 * In backends (as soon as forked) we operate the underlying socket in
	 * nonblocking mode and use latches to implement blocking semantics if
	 * needed. That allows us to provide safely interruptible reads and
	 * writes.
	 */
#ifndef WIN32
	if (!pg_set_noblock(port->sock))
		ereport(FATAL,
				(errmsg("could not set socket to nonblocking mode: %m")));
#endif

#ifndef WIN32

	/* Don't give the socket to any subprograms we execute. */
	if (fcntl(port->sock, F_SETFD, FD_CLOEXEC) < 0)
		elog(FATAL, "fcntl(F_SETFD) failed on socket: %m");
#endif

	if (socket_aio)
	{
		FeBeWaitSet =
			CreateWaitEventSetExtended(NULL,
									   FeBeWaitSetNEvents,
									   WAIT_EVENT_SET_NATIVE_SOCKET_AIO);
		socket_pos = AddWaitEventToSet(FeBeWaitSet, 0,
									   port->sock, NULL, NULL);
	}
	else
	{
		FeBeWaitSet = CreateWaitEventSet(NULL, FeBeWaitSetNEvents);
		socket_pos = AddWaitEventToSet(FeBeWaitSet, WL_SOCKET_WRITEABLE,
									   port->sock, NULL, NULL);
	}
	latch_pos = AddWaitEventToSet(FeBeWaitSet, WL_LATCH_SET, PGINVALID_SOCKET,
								  MyLatch, NULL);
	AddWaitEventToSet(FeBeWaitSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	port->wes = FeBeWaitSet;
	port->sock_pos = socket_pos;

	/*
	 * The event positions match the order we added them, but let's sanity
	 * check them to be sure.
	 */
	Assert(socket_pos == FeBeWaitSetSocketPos);
	Assert(latch_pos == FeBeWaitSetLatchPos);

	return port;
}

/* --------------------------------
 *		socket_comm_reset - reset libpq during error recovery
 *
 * This is called from error recovery at the outer idle loop.  It's
 * just to get us out of trouble if we somehow manage to elog() from
 * inside a pqcomm.c routine (which ideally will never happen, but...)
 * --------------------------------
 */
static void
socket_comm_reset(void)
{
	/* Do not throw away pending data, but do reset the busy flag */
	PqCommBusy = false;
}

/* --------------------------------
 *		socket_close - shutdown libpq at backend exit
 *
 * This is the one pg_on_exit_callback in place during BackendInitialize().
 * That function's unusual signal handling constrains that this callback be
 * safe to run at any instant.
 * --------------------------------
 */
static void
socket_close(int code, Datum arg)
{
	/* Nothing to do in a standalone backend, where MyProcPort is NULL. */
	if (MyProcPort != NULL)
	{
#ifdef ENABLE_GSS
		/*
		 * Shutdown GSSAPI layer.  This section does nothing when interrupting
		 * BackendInitialize(), because pg_GSS_recvauth() makes first use of
		 * "ctx" and "cred".
		 *
		 * Note that we don't bother to free MyProcPort->gss, since we're
		 * about to exit anyway.
		 */
		if (MyProcPort->gss)
		{
			OM_uint32	min_s;

			if (MyProcPort->gss->ctx != GSS_C_NO_CONTEXT)
				gss_delete_sec_context(&min_s, &MyProcPort->gss->ctx, NULL);

			if (MyProcPort->gss->cred != GSS_C_NO_CREDENTIAL)
				gss_release_cred(&min_s, &MyProcPort->gss->cred);
		}
#endif							/* ENABLE_GSS */

		/*
		 * Cleanly shut down SSL layer.  Nowhere else does a postmaster child
		 * call this, so this is safe when interrupting BackendInitialize().
		 */
		secure_close(MyProcPort);

		/*
		 * Formerly we did an explicit close() here, but it seems better to
		 * leave the socket open until the process dies.  This allows clients
		 * to perform a "synchronous close" if they care --- wait till the
		 * transport layer reports connection closure, and you can be sure the
		 * backend has exited.
		 *
		 * We do set sock to PGINVALID_SOCKET to prevent any further I/O,
		 * though.
		 */
		MyProcPort->sock = PGINVALID_SOCKET;
	}
}



/* --------------------------------
 * Postmaster functions to handle sockets.
 * --------------------------------
 */

/*
 * ListenServerPort -- open a "listening" port to accept connections.
 *
 * family should be AF_UNIX or AF_UNSPEC; portNumber is the port number.
 * For AF_UNIX ports, hostName should be NULL and unixSocketDir must be
 * specified.  For TCP ports, hostName is either NULL for all interfaces or
 * the interface to listen on, and unixSocketDir is ignored (can be NULL).
 *
 * Successfully opened sockets are appended to the ListenSockets[] array.  On
 * entry, *NumListenSockets holds the number of elements currently in the
 * array, and it is updated to reflect the opened sockets.  MaxListen is the
 * allocated size of the array.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
ListenServerPort(int family, const char *hostName, unsigned short portNumber,
				 const char *unixSocketDir,
				 pgsocket ListenSockets[], int *NumListenSockets, int MaxListen)
{
	pgsocket	fd;
	int			err;
	int			maxconn;
	int			ret;
	char		portNumberStr[32];
	const char *familyDesc;
	char		familyDescBuf[64];
	const char *addrDesc;
	char		addrBuf[NI_MAXHOST];
	char	   *service;
	struct addrinfo *addrs = NULL,
			   *addr;
	struct addrinfo hint;
	int			added = 0;
	char		unixSocketPath[MAXPGPATH];
#if !defined(WIN32) || defined(IPV6_V6ONLY)
	int			one = 1;
#endif

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = family;
	hint.ai_flags = AI_PASSIVE;
	hint.ai_socktype = SOCK_STREAM;

	if (family == AF_UNIX)
	{
		/*
		 * Create unixSocketPath from portNumber and unixSocketDir and lock
		 * that file path
		 */
		UNIXSOCK_PATH(unixSocketPath, portNumber, unixSocketDir);
		if (strlen(unixSocketPath) >= UNIXSOCK_PATH_BUFLEN)
		{
			ereport(LOG,
					(errmsg("Unix-domain socket path \"%s\" is too long (maximum %d bytes)",
							unixSocketPath,
							(int) (UNIXSOCK_PATH_BUFLEN - 1))));
			return STATUS_ERROR;
		}
		if (Lock_AF_UNIX(unixSocketDir, unixSocketPath) != STATUS_OK)
			return STATUS_ERROR;
		service = unixSocketPath;
	}
	else
	{
		snprintf(portNumberStr, sizeof(portNumberStr), "%d", portNumber);
		service = portNumberStr;
	}

	ret = pg_getaddrinfo_all(hostName, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (hostName)
			ereport(LOG,
					(errmsg("could not translate host name \"%s\", service \"%s\" to address: %s",
							hostName, service, gai_strerror(ret))));
		else
			ereport(LOG,
					(errmsg("could not translate service \"%s\" to address: %s",
							service, gai_strerror(ret))));
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	for (addr = addrs; addr; addr = addr->ai_next)
	{
		if (family != AF_UNIX && addr->ai_family == AF_UNIX)
		{
			/*
			 * Only set up a unix domain socket when they really asked for it.
			 * The service/port is different in that case.
			 */
			continue;
		}

		/* See if there is still room to add 1 more socket. */
		if (*NumListenSockets == MaxListen)
		{
			ereport(LOG,
					(errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded",
							MaxListen)));
			break;
		}

		/* set up address family name for log messages */
		switch (addr->ai_family)
		{
			case AF_INET:
				familyDesc = _("IPv4");
				break;
			case AF_INET6:
				familyDesc = _("IPv6");
				break;
			case AF_UNIX:
				familyDesc = _("Unix");
				break;
			default:
				snprintf(familyDescBuf, sizeof(familyDescBuf),
						 _("unrecognized address family %d"),
						 addr->ai_family);
				familyDesc = familyDescBuf;
				break;
		}

		/* set up text form of address for log messages */
		if (addr->ai_family == AF_UNIX)
			addrDesc = unixSocketPath;
		else
		{
			pg_getnameinfo_all((const struct sockaddr_storage *) addr->ai_addr,
							   addr->ai_addrlen,
							   addrBuf, sizeof(addrBuf),
							   NULL, 0,
							   NI_NUMERICHOST);
			addrDesc = addrBuf;
		}

		if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not create %s socket for address \"%s\": %m",
							familyDesc, addrDesc)));
			continue;
		}

#ifndef WIN32
		/* Don't give the listen socket to any subprograms we execute. */
		if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
			elog(FATAL, "fcntl(F_SETFD) failed on socket: %m");

		/*
		 * Without the SO_REUSEADDR flag, a new postmaster can't be started
		 * right away after a stop or crash, giving "address already in use"
		 * error on TCP ports.
		 *
		 * On win32, however, this behavior only happens if the
		 * SO_EXCLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows
		 * multiple servers to listen on the same address, resulting in
		 * unpredictable behavior. With no flags at all, win32 behaves as Unix
		 * with SO_REUSEADDR.
		 */
		if (addr->ai_family != AF_UNIX)
		{
			if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
							(char *) &one, sizeof(one))) == -1)
			{
				ereport(LOG,
						(errcode_for_socket_access(),
				/* translator: third %s is IPv4, IPv6, or Unix */
						 errmsg("%s(%s) failed for %s address \"%s\": %m",
								"setsockopt", "SO_REUSEADDR",
								familyDesc, addrDesc)));
				closesocket(fd);
				continue;
			}
		}
#endif

#ifdef IPV6_V6ONLY
		if (addr->ai_family == AF_INET6)
		{
			if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY,
						   (char *) &one, sizeof(one)) == -1)
			{
				ereport(LOG,
						(errcode_for_socket_access(),
				/* translator: third %s is IPv4, IPv6, or Unix */
						 errmsg("%s(%s) failed for %s address \"%s\": %m",
								"setsockopt", "IPV6_V6ONLY",
								familyDesc, addrDesc)));
				closesocket(fd);
				continue;
			}
		}
#endif

		/*
		 * Note: This might fail on some OS's, like Linux older than
		 * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
		 * ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all ipv4
		 * connections.
		 */
		err = bind(fd, addr->ai_addr, addr->ai_addrlen);
		if (err < 0)
		{
			int			saved_errno = errno;

			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not bind %s address \"%s\": %m",
							familyDesc, addrDesc),
					 saved_errno == EADDRINUSE ?
					 (addr->ai_family == AF_UNIX ?
					  errhint("Is another postmaster already running on port %d?",
							  (int) portNumber) :
					  errhint("Is another postmaster already running on port %d?"
							  " If not, wait a few seconds and retry.",
							  (int) portNumber)) : 0));
			closesocket(fd);
			continue;
		}

		if (addr->ai_family == AF_UNIX)
		{
			if (Setup_AF_UNIX(service) != STATUS_OK)
			{
				closesocket(fd);
				break;
			}
		}

		/*
		 * Select appropriate accept-queue length limit.  It seems reasonable
		 * to use a value similar to the maximum number of child processes
		 * that the postmaster will permit.
		 */
		maxconn = MaxConnections * 2;

		err = listen(fd, maxconn);
		if (err < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not listen on %s address \"%s\": %m",
							familyDesc, addrDesc)));
			closesocket(fd);
			continue;
		}

		if (addr->ai_family == AF_UNIX)
			ereport(LOG,
					(errmsg("listening on Unix socket \"%s\"",
							addrDesc)));
		else
			ereport(LOG,
			/* translator: first %s is IPv4 or IPv6 */
					(errmsg("listening on %s address \"%s\", port %d",
							familyDesc, addrDesc, (int) portNumber)));

		ListenSockets[*NumListenSockets] = fd;
		(*NumListenSockets)++;
		added++;
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);

	if (!added)
		return STATUS_ERROR;

	return STATUS_OK;
}


/*
 * Lock_AF_UNIX -- configure unix socket file path
 */
static int
Lock_AF_UNIX(const char *unixSocketDir, const char *unixSocketPath)
{
	/* no lock file for abstract sockets */
	if (unixSocketPath[0] == '@')
		return STATUS_OK;

	/*
	 * Grab an interlock file associated with the socket file.
	 *
	 * Note: there are two reasons for using a socket lock file, rather than
	 * trying to interlock directly on the socket itself.  First, it's a lot
	 * more portable, and second, it lets us remove any pre-existing socket
	 * file without race conditions.
	 */
	CreateSocketLockFile(unixSocketPath, true, unixSocketDir);

	/*
	 * Once we have the interlock, we can safely delete any pre-existing
	 * socket file to avoid failure at bind() time.
	 */
	(void) unlink(unixSocketPath);

	/*
	 * Remember socket file pathnames for later maintenance.
	 */
	sock_paths = lappend(sock_paths, pstrdup(unixSocketPath));

	return STATUS_OK;
}


/*
 * Setup_AF_UNIX -- configure unix socket permissions
 */
static int
Setup_AF_UNIX(const char *sock_path)
{
	/* no file system permissions for abstract sockets */
	if (sock_path[0] == '@')
		return STATUS_OK;

	/*
	 * Fix socket ownership/permission if requested.  Note we must do this
	 * before we listen() to avoid a window where unwanted connections could
	 * get accepted.
	 */
	Assert(Unix_socket_group);
	if (Unix_socket_group[0] != '\0')
	{
#ifdef WIN32
		elog(WARNING, "configuration item \"unix_socket_group\" is not supported on this platform");
#else
		char	   *endptr;
		unsigned long val;
		gid_t		gid;

		val = strtoul(Unix_socket_group, &endptr, 10);
		if (*endptr == '\0')
		{						/* numeric group id */
			gid = val;
		}
		else
		{						/* convert group name to id */
			struct group *gr;

			gr = getgrnam(Unix_socket_group);
			if (!gr)
			{
				ereport(LOG,
						(errmsg("group \"%s\" does not exist",
								Unix_socket_group)));
				return STATUS_ERROR;
			}
			gid = gr->gr_gid;
		}
		if (chown(sock_path, -1, gid) == -1)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not set group of file \"%s\": %m",
							sock_path)));
			return STATUS_ERROR;
		}
#endif
	}

	if (chmod(sock_path, Unix_socket_permissions) == -1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not set permissions of file \"%s\": %m",
						sock_path)));
		return STATUS_ERROR;
	}
	return STATUS_OK;
}


/*
 * AcceptConnection -- accept a new connection with client using
 *		server port.  Fills *client_sock with the FD and endpoint info
 *		of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster waits for the socket to be ready to accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
AcceptConnection(pgsocket server_fd, ClientSocket *client_sock)
{
	/* accept connection and fill in the client (remote) address */
	client_sock->raddr.salen = sizeof(client_sock->raddr.addr);
	if ((client_sock->sock = accept(server_fd,
									(struct sockaddr *) &client_sock->raddr.addr,
									&client_sock->raddr.salen)) == PGINVALID_SOCKET)
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not accept new connection: %m")));

		/*
		 * If accept() fails then postmaster.c will still see the server
		 * socket as read-ready, and will immediately try again.  To avoid
		 * uselessly sucking lots of CPU, delay a bit before trying again.
		 * (The most likely reason for failure is being out of kernel file
		 * table slots; we can do little except hope some will get freed up.)
		 */
		pg_usleep(100000L);		/* wait 0.1 sec */
		return STATUS_ERROR;
	}

	return STATUS_OK;
}

/*
 * TouchSocketFiles -- mark socket files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * files have a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves them from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
void
TouchSocketFiles(void)
{
	ListCell   *l;

	/* Loop through all created sockets... */
	foreach(l, sock_paths)
	{
		char	   *sock_path = (char *) lfirst(l);

		/* Ignore errors; there's no point in complaining */
		(void) utime(sock_path, NULL);
	}
}

/*
 * RemoveSocketFiles -- unlink socket files at postmaster shutdown
 */
void
RemoveSocketFiles(void)
{
	ListCell   *l;

	/* Loop through all created sockets... */
	foreach(l, sock_paths)
	{
		char	   *sock_path = (char *) lfirst(l);

		/* Ignore any error. */
		(void) unlink(sock_path);
	}
	/* Since we're about to exit, no need to reclaim storage */
	sock_paths = NIL;
}


/* --------------------------------
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int
pq_getbyte(void)
{
	ssize_t		r;
	char		byte;

	Assert(PqCommReadingMsg);

	r = port_recv(MyProcPort, &byte, 1, WAIT_EVENT_CLIENT_READ);

	return r < 1 ? EOF : byte;
}

/* --------------------------------
 *		pq_peekbyte		- peek at next byte from connection
 *
 *	 Same as pq_getbyte() except we don't consume the byte.
 * --------------------------------
 */
int
pq_peekbyte(void)
{
	return port_peek(MyProcPort);
}

/* --------------------------------
 *		pq_getbyte_if_available - get a single byte from connection,
 *			if available
 *
 * The received byte is stored in *c. Returns 1 if a byte was read,
 * 0 if no data was available, or EOF if trouble.
 * --------------------------------
 */
int
pq_getbyte_if_available(unsigned char *c)
{
	int			r;

	Assert(PqCommReadingMsg);

	r = port_recv(MyProcPort, c, 1, 0);
	if (r < 0)
	{
		/*
		 * Ok if no data available without blocking or interrupted (though
		 * EINTR really shouldn't happen). Report other errors.
		 */
		if (errno == EINPROGRESS)
		{
			r = 0;
		}
		else if (errno == EINTR)
		{
			r = 0;
		}
		else
		{
			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If errno is zero, assume it's EOF and let the caller complain.
			 */
			if (errno != 0)
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not receive data from client: %m")));
			r = EOF;
		}
	}
	else if (r == 0)
	{
		/* EOF detected */
		r = EOF;
	}

	return r;
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getbytes(char *s, size_t len)
{
	Assert(PqCommReadingMsg);

	return port_recv_all(MyProcPort, s, len, WAIT_EVENT_CLIENT_READ) < 0 ? EOF : 0;
}

/* --------------------------------
 *		pq_discardbytes		- throw away a known number of bytes
 *
 *		same as pq_getbytes except we do not copy the data to anyplace.
 *		this is used for resynchronizing after read errors.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pq_discardbytes(size_t len)
{
	char		buf[1024];
	size_t		amount;

	Assert(PqCommReadingMsg);

	while (len > 0)
	{
		amount = Min(lengthof(buf), len);
		if (pq_getbytes(buf, amount) == EOF)
			return EOF;
		len -= amount;
	}
	return 0;
}

/* --------------------------------
 *		pq_buffer_remaining_data	- return number of bytes in receive buffer
 *
 * This will *not* attempt to read more data. And reading up to that number of
 * bytes should not cause reading any more data either.
 * --------------------------------
 */
ssize_t
pq_buffer_remaining_data(void)
{
	return port_recv_pending(MyProcPort);
}


/* --------------------------------
 *		pq_startmsgread - begin reading a message from the client.
 *
 *		This must be called before any of the pq_get* functions.
 * --------------------------------
 */
void
pq_startmsgread(void)
{
	/*
	 * There shouldn't be a read active already, but let's check just to be
	 * sure.
	 */
	if (PqCommReadingMsg)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("terminating connection because protocol synchronization was lost")));

	PqCommReadingMsg = true;
}


/* --------------------------------
 *		pq_endmsgread	- finish reading message.
 *
 *		This must be called after reading a message with pq_getbytes()
 *		and friends, to indicate that we have read the whole message.
 *		pq_getmessage() does this implicitly.
 * --------------------------------
 */
void
pq_endmsgread(void)
{
	Assert(PqCommReadingMsg);

	PqCommReadingMsg = false;
}

/* --------------------------------
 *		pq_is_reading_msg - are we currently reading a message?
 *
 * This is used in error recovery at the outer idle loop to detect if we have
 * lost protocol sync, and need to terminate the connection. pq_startmsgread()
 * will check for that too, but it's nicer to detect it earlier.
 * --------------------------------
 */
bool
pq_is_reading_msg(void)
{
	return PqCommReadingMsg;
}

/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		maxlen is the upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getmessage(StringInfo s, int maxlen)
{
	int32		len;

	Assert(PqCommReadingMsg);

	resetStringInfo(s);

	/* Read message length word */
	if (pq_getbytes((char *) &len, 4) == EOF)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = pg_ntoh32(len);

	if (len < 4 || len > maxlen)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.  If we run out of room (ridiculously
		 * large message), we will elog(ERROR), but we want to discard the
		 * message body so as not to lose communication sync.
		 */
		PG_TRY();
		{
			enlargeStringInfo(s, len);
		}
		PG_CATCH();
		{
			if (pq_discardbytes(len) == EOF)
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));

			/* we discarded the rest of the message so we're back in sync. */
			PqCommReadingMsg = false;
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* And grab the message */
		if (pq_getbytes(s->data, len) == EOF)
		{
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete message from client")));
			return EOF;
		}
		s->len = len;
		/* Place a trailing null per StringInfo convention */
		s->data[len] = '\0';
	}

	/* finished reading the message. */
	PqCommReadingMsg = false;

	return 0;
}

/* --------------------------------
 *		socket_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
socket_flush(void)
{
	return port_flush(MyProcPort, WAIT_EVENT_CLIENT_WRITE) == 0 ? 0 : EOF;
}

/* --------------------------------
 * Message-level I/O routines begin here.
 * --------------------------------
 */


/* --------------------------------
 *		socket_putmessage - send a normal message (suppressed in COPY OUT mode)
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		len is the length of the message body data at *s.  A message length
 *		word (equal to len+4 because it counts itself too) is inserted by this
 *		routine.
 *
 *		We suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
socket_putmessage(char msgtype, const char *s, size_t len)
{
	Port	   *port = MyProcPort;
	uint32		n32;

	Assert(msgtype != 0);

	if (PqCommBusy)
		return 0;
	PqCommBusy = true;
	if (port_send_all(port, &msgtype, 1, 0) == EOF)
		goto fail;

	n32 = pg_hton32((uint32) (len + 4));
	if (port_send_all(port, (char *) &n32, 4, 0) == EOF)
		goto fail;

	if (port_send_all(port, s, len, 0) == EOF)
		goto fail;
	PqCommBusy = false;
	return 0;

fail:
	PqCommBusy = false;
	return EOF;
}

/* --------------------------------
 *		pq_putmessage_noblock	- like pq_putmessage, but never blocks
 *
 *		If the output buffer is too small to hold the message, the buffer
 *		is enlarged. XXX?
 */
static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
{
	/* XXX add more buffers? */
	socket_putmessage(msgtype, s, len);
}

/* --------------------------------
 *		pq_putmessage_v2 - send a message in protocol version 2
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		We no longer support protocol version 2, but we have kept this
 *		function so that if a client tries to connect with protocol version 2,
 *		as a courtesy we can still send the "unsupported protocol version"
 *		error to the client in the old format.
 *
 *		Like in pq_putmessage(), we suppress messages generated while
 *		pqcomm.c is busy.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_putmessage_v2(char msgtype, const char *s, size_t len)
{
	Port	   *port = MyProcPort;

	Assert(msgtype != 0);

	if (PqCommBusy)
		return 0;
	PqCommBusy = true;
	if (port_send_all(port, &msgtype, 1, 0))
		goto fail;

	if (port_send_all(port, s, len, 0))
		goto fail;
	PqCommBusy = false;
	return 0;

fail:
	PqCommBusy = false;
	return EOF;
}

/*
 * Support for TCP Keepalive parameters
 */

/*
 * On Windows, we need to set both idle and interval at the same time.
 * We also cannot reset them to the default (setting to zero will
 * actually set them to zero, not default), therefore we fallback to
 * the out-of-the-box default instead.
 */
#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
static int
pq_setkeepaliveswin32(Port *port, int idle, int interval)
{
	struct tcp_keepalive ka;
	DWORD		retsize;

	if (idle <= 0)
		idle = 2 * 60 * 60;		/* default = 2 hours */
	if (interval <= 0)
		interval = 1;			/* default = 1 second */

	ka.onoff = 1;
	ka.keepalivetime = idle * 1000;
	ka.keepaliveinterval = interval * 1000;

	if (WSAIoctl(port->sock,
				 SIO_KEEPALIVE_VALS,
				 (LPVOID) &ka,
				 sizeof(ka),
				 NULL,
				 0,
				 &retsize,
				 NULL,
				 NULL)
		!= 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: error code %d",
						"WSAIoctl", "SIO_KEEPALIVE_VALS", WSAGetLastError())));
		return STATUS_ERROR;
	}
	if (port->keepalives_idle != idle)
		port->keepalives_idle = idle;
	if (port->keepalives_interval != interval)
		port->keepalives_interval = interval;
	return STATUS_OK;
}
#endif

int
pq_getkeepalivesidle(Port *port)
{
#if defined(PG_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_idle != 0)
		return port->keepalives_idle;

	if (port->default_keepalives_idle == 0)
	{
#ifndef WIN32
		socklen_t	size = sizeof(port->default_keepalives_idle);

		if (getsockopt(port->sock, IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE,
					   (char *) &port->default_keepalives_idle,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", PG_TCP_KEEPALIVE_IDLE_STR)));
			port->default_keepalives_idle = -1; /* don't know */
		}
#else							/* WIN32 */
		/* We can't get the defaults on Windows, so return "don't know" */
		port->default_keepalives_idle = -1;
#endif							/* WIN32 */
	}

	return port->default_keepalives_idle;
#else
	return 0;
#endif
}

int
pq_setkeepalivesidle(int idle, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

/* check SIO_KEEPALIVE_VALS here, not just WIN32, as some toolchains lack it */
#if defined(PG_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
	if (idle == port->keepalives_idle)
		return STATUS_OK;

#ifndef WIN32
	if (port->default_keepalives_idle <= 0)
	{
		if (pq_getkeepalivesidle(port) < 0)
		{
			if (idle == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (idle == 0)
		idle = port->default_keepalives_idle;

	if (setsockopt(port->sock, IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE,
				   (char *) &idle, sizeof(idle)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", PG_TCP_KEEPALIVE_IDLE_STR)));
		return STATUS_ERROR;
	}

	port->keepalives_idle = idle;
#else							/* WIN32 */
	return pq_setkeepaliveswin32(port, idle, port->keepalives_interval);
#endif
#else
	if (idle != 0)
	{
		ereport(LOG,
				(errmsg("setting the keepalive idle time is not supported")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivesinterval(Port *port)
{
#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_interval != 0)
		return port->keepalives_interval;

	if (port->default_keepalives_interval == 0)
	{
#ifndef WIN32
		socklen_t	size = sizeof(port->default_keepalives_interval);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
					   (char *) &port->default_keepalives_interval,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_KEEPINTVL")));
			port->default_keepalives_interval = -1; /* don't know */
		}
#else
		/* We can't get the defaults on Windows, so return "don't know" */
		port->default_keepalives_interval = -1;
#endif							/* WIN32 */
	}

	return port->default_keepalives_interval;
#else
	return 0;
#endif
}

int
pq_setkeepalivesinterval(int interval, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
	if (interval == port->keepalives_interval)
		return STATUS_OK;

#ifndef WIN32
	if (port->default_keepalives_interval <= 0)
	{
		if (pq_getkeepalivesinterval(port) < 0)
		{
			if (interval == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (interval == 0)
		interval = port->default_keepalives_interval;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
				   (char *) &interval, sizeof(interval)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_KEEPINTVL")));
		return STATUS_ERROR;
	}

	port->keepalives_interval = interval;
#else							/* WIN32 */
	return pq_setkeepaliveswin32(port, port->keepalives_idle, interval);
#endif
#else
	if (interval != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_KEEPINTVL")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivescount(Port *port)
{
#ifdef TCP_KEEPCNT
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_count != 0)
		return port->keepalives_count;

	if (port->default_keepalives_count == 0)
	{
		socklen_t	size = sizeof(port->default_keepalives_count);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
					   (char *) &port->default_keepalives_count,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_KEEPCNT")));
			port->default_keepalives_count = -1;	/* don't know */
		}
	}

	return port->default_keepalives_count;
#else
	return 0;
#endif
}

int
pq_setkeepalivescount(int count, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#ifdef TCP_KEEPCNT
	if (count == port->keepalives_count)
		return STATUS_OK;

	if (port->default_keepalives_count <= 0)
	{
		if (pq_getkeepalivescount(port) < 0)
		{
			if (count == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (count == 0)
		count = port->default_keepalives_count;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
				   (char *) &count, sizeof(count)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_KEEPCNT")));
		return STATUS_ERROR;
	}

	port->keepalives_count = count;
#else
	if (count != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_KEEPCNT")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_gettcpusertimeout(Port *port)
{
#ifdef TCP_USER_TIMEOUT
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->tcp_user_timeout != 0)
		return port->tcp_user_timeout;

	if (port->default_tcp_user_timeout == 0)
	{
		socklen_t	size = sizeof(port->default_tcp_user_timeout);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
					   (char *) &port->default_tcp_user_timeout,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_USER_TIMEOUT")));
			port->default_tcp_user_timeout = -1;	/* don't know */
		}
	}

	return port->default_tcp_user_timeout;
#else
	return 0;
#endif
}

int
pq_settcpusertimeout(int timeout, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#ifdef TCP_USER_TIMEOUT
	if (timeout == port->tcp_user_timeout)
		return STATUS_OK;

	if (port->default_tcp_user_timeout <= 0)
	{
		if (pq_gettcpusertimeout(port) < 0)
		{
			if (timeout == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (timeout == 0)
		timeout = port->default_tcp_user_timeout;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
				   (char *) &timeout, sizeof(timeout)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_USER_TIMEOUT")));
		return STATUS_ERROR;
	}

	port->tcp_user_timeout = timeout;
#else
	if (timeout != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_USER_TIMEOUT")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

/*
 * GUC assign_hook for tcp_keepalives_idle
 */
void
assign_tcp_keepalives_idle(int newval, void *extra)
{
	/*
	 * The kernel API provides no way to test a value without setting it; and
	 * once we set it we might fail to unset it.  So there seems little point
	 * in fully implementing the check-then-assign GUC API for these
	 * variables.  Instead we just do the assignment on demand.
	 * pq_setkeepalivesidle reports any problems via ereport(LOG).
	 *
	 * This approach means that the GUC value might have little to do with the
	 * actual kernel value, so we use a show_hook that retrieves the kernel
	 * value rather than trusting GUC's copy.
	 */
	(void) pq_setkeepalivesidle(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_idle
 */
const char *
show_tcp_keepalives_idle(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesidle(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_keepalives_interval
 */
void
assign_tcp_keepalives_interval(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivesinterval(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_interval
 */
const char *
show_tcp_keepalives_interval(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesinterval(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_keepalives_count
 */
void
assign_tcp_keepalives_count(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivescount(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_count
 */
const char *
show_tcp_keepalives_count(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivescount(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_user_timeout
 */
void
assign_tcp_user_timeout(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_settcpusertimeout(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_user_timeout
 */
const char *
show_tcp_user_timeout(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_gettcpusertimeout(MyProcPort));
	return nbuf;
}

/*
 * Check if the client is still connected.
 */
bool
pq_check_connection(void)
{
	WaitEvent	events[FeBeWaitSetNEvents];
	int			rc;

	/*
	 * It's OK to modify the socket event filter without restoring, because
	 * all FeBeWaitSet socket wait sites do the same.
	 */
	ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, WL_SOCKET_CLOSED, NULL);

retry:
	rc = WaitEventSetWait(FeBeWaitSet, 0, events, lengthof(events), 0);
	for (int i = 0; i < rc; ++i)
	{
		if (events[i].events & WL_SOCKET_CLOSED)
			return false;
		if (events[i].events & WL_LATCH_SET)
		{
			/*
			 * A latch event might be preventing other events from being
			 * reported.  Reset it and poll again.  No need to restore it
			 * because no code should expect latches to survive across
			 * CHECK_FOR_INTERRUPTS().
			 */
			ResetLatch(MyLatch);
			goto retry;
		}
	}

	return true;
}
