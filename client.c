#include "libpq-fe.h"

static void
do_ios_(PGconn *conn)
{
	PGIOOp *ios[2];
	int nios;

	nios = PQios(conn, ios, lengthof(ios));

#ifndef USE_IO_URING
	/*
	 * Example of handling IO requests synchronously.  Something like this
	 * might be useful if the goal is to interpose different calls.  Otherwise,
	 * it's just a way of demonstrating the PQios interface with portable code.
	 */
	for (int i = 0; i < nios; ++i) {
		switch (ios[i].op) {
		case PQIO_OP_CONNECT:
			ios[i].result = connect(ios[i].u.connect_args.s,
									ios[i].u.connect_args.name,
									ios[i].u.connect_args.namelen);
			ios[i].error = errno;
			break;
		case PQIO_OP_RECV:
			ios[i].result = recv(ios[i].u.recv_args.s,
								 ios[i].u.recv_args.buf,
								 ios[i].u.recv_args.len,
								 ios[i].u.recv_args.flags);
			ios[i].error = errno;
			break;
		case PQIO_OP_SEND:
			ios[i].result = send(ios[i].u.send_args.s,
								 ios[i].u.send_args.buf,
								 ios[i].u.send_args.len,
								 ios[i].u.send_args.flags);
			ios[i].error = errno;
			break;
		default:
			ios[i].result = -1;
			ios[i].error = ENOSYS;		/* unexpected */
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
	conn = PQconnectStart("dbname = postgres");
	if (conn == NULL) {
		fprintf(stderr, "out of memory\n");
		return EXIT_FAILURE;
	}

	/* Handle bad connection string format etc. */
	if (PQstatus(conn) == CONNECTION_BAD) {
		fprintf(stderr, "connection bad before we even begin\n");
		return EXIT_FAILURE;
	}

	/* Process IO until we reach connection OK or failed state. */
	while (PQstatus(conn) != CONNECTION_OK) {
		do_ios(conn);
		if (PQconnectPoll(conn) == PGRESS_POLLING_FAILED) {
			fprintf(stderr, "failed to connect\n");
			return EXIT_FAILURE;
		}
	}

	/* Now send a query.  This doesn't really send anything yet... */
	if (PQsendQuery(conn, "SELECT 42") != 1) {
		fprintf(stderr, "PQsendQuery failed\n");
		return EXIT_FAILURE;
	}

	/*
	 * Now perform the socket IO.  This will send() and then recv() until we
	 * have results.
	 */
	while (PQisbusy(conn)) {
		do_ios(conn);
		PQconsumeInput(conn);
	}
}
