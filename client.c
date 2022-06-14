#include "libpq-fe.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>

static void
do_ios(PGconn *conn)
{
	PQIO *ios[2];
	int nios;

	nios = PQpendingIo(conn, ios, sizeof(ios) / sizeof(ios[0]));

printf("got %d ios\n", nios);
#ifndef USE_IO_URING
	/*
	 * Example of handling IO requests synchronously.  Something like this
	 * might be useful if the goal is to interpose different calls.  Otherwise,
	 * it's just a way of demonstrating the PQios interface with portable code.
	 */
	for (int i = 0; i < nios; ++i) {
		PQIO *io = ios[i];
		switch (io->op) {
		case PQIO_OP_CONNECT:
			io->result = connect(io->u.connect_args.s,
								 io->u.connect_args.name,
								 io->u.connect_args.namelen);
			io->error = errno;
printf("connect -> %s\n", io->result);
			break;
		case PQIO_OP_RECV:
			io->result = recv(io->u.recv_args.s,
							  io->u.recv_args.buf,
							  io->u.recv_args.len,
							  io->u.recv_args.flags);
			io->error = errno;
printf("recv -> %s\n", io->result);
			break;
		case PQIO_OP_SEND:
			io->result = send(io->u.send_args.s,
							  io->u.send_args.buf,
							  io->u.send_args.len,
							  io->u.send_args.flags);
			io->error = errno;
printf("send -> %s\n", io->result);
			break;
		default:
			io->result = -1;
			io->error = ENOSYS;		/* unexpected */
			break;
		}
	}
#endif
}

int
main()
{
	PGconn *conn;
	PQIOOp *ios[2];
	bool connected = false;

	/*
	 * Try to start a connection.  This doesn't actually do any socket IO,
	 * yet.
	 */
printf("1\n");
	conn = PQconnectStartExternalIO("dbname = postgres");
	if (conn == NULL) {
		fprintf(stderr, "out of memory\n");
		return EXIT_FAILURE;
	}

printf("2\n");
	/* Handle bad connection string format etc. */
	if (PQstatus(conn) == CONNECTION_BAD) {
		fprintf(stderr, "connection bad before we even begin: %s\n",
				PQerrorMessage(conn));
		return EXIT_FAILURE;
	}

printf("3\n");
	/* Process IO until we reach connection OK or failed state. */
	while (PQstatus(conn) != CONNECTION_OK) {
printf("3.1\n");
		do_ios(conn);
		if (PQconnectPoll(conn) == PGRES_POLLING_FAILED) {
			fprintf(stderr, "failed to connect\n");
			return EXIT_FAILURE;
		}
	}

printf("4\n");
	/* Now send a query.  This doesn't really send anything yet... */
	if (PQsendQuery(conn, "SELECT 42") != 1) {
		fprintf(stderr, "PQsendQuery failed\n");
		return EXIT_FAILURE;
	}

printf("5\n");
	/*
	 * Now perform the socket IO.  This will send() and then recv() until we
	 * have results.
	 */
	while (PQisBusy(conn)) {
		do_ios(conn);
		PQconsumeInput(conn);
	}
}
