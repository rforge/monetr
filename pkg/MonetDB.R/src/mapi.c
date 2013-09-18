#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <assert.h>
#include <fcntl.h>

// R headers
#include <R.h>
#include <Rdefines.h>

#define BLOCKSIZE 8190
#define BUFSIZE BLOCKSIZE+1
#define SOCKET int
#define SOCK_ATTR "mapi_conn_do_not_touch"
#define CONN_CLASS "monetdb_mapi_conn"
#define TRUE 1
#define FALSE 0
#define ALLOCSIZE 1048576 // 1 MB
#define DEBUG FALSE

SEXP mapiConnect(SEXP host, SEXP port, SEXP timeout) {
	// be a bit paranoid about the parameters
	assert(IS_CHARACTER(host));
	assert(GET_LENGTH(host) == 1);
	assert(IS_INTEGER(port));
	assert(GET_LENGTH(port) == 1);
	assert(IS_INTEGER(timeout));
	assert(GET_LENGTH(timeout) == 1);

	const char *hostval = CHAR(STRING_ELT(host, 0));
	const int portval = INTEGER_POINTER(AS_INTEGER(port))[0];
	const int timeoutval = INTEGER_POINTER(AS_INTEGER(port))[0];

	assert(strlen(hostval) > 0);
	assert(portval > 0 && portval < 65535);
	assert(timeoutval > 0);

	SEXP connobj, class, attr;
	SOCKET sock;
	struct sockaddr_in serveraddr;
	struct hostent *server;

	// resolve server address
	server = gethostbyname(hostval);
	if (server == NULL) {
		error("XX: ERROR, no such host as %s\n", hostval);
	}

	bzero((char *) &serveraddr, sizeof(serveraddr));
	serveraddr.sin_family = AF_INET;
	bcopy((char *) server->h_addr, (char *) &serveraddr.sin_addr.s_addr,
			server->h_length);
	serveraddr.sin_port = htons(portval);

	// create socket
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		error("XX: ERROR opening socket");
	}

	// actually connect
	if (connect(sock, (const struct sockaddr *) &serveraddr, sizeof(serveraddr))
			< 0) {
		error("XX: ERROR connecting to %s:%i\n", hostval, portval);
	}

	// setting send/receive timeouts for socket
	struct timeval sto;
	sto.tv_sec = timeoutval;
	sto.tv_usec = 0;
	if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &sto, sizeof(sto))
			< 0)
		error("XX: setsockopt failed\n");
	if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &sto, sizeof(sto))
			< 0)
		error("XX: setsockopt failed\n");

	// construct a r object of class monetdb_mapi_conn with an attribute holding the connection id
	PROTECT(connobj = ScalarInteger(1));
	PROTECT(attr = ScalarInteger(1));
	PROTECT(class = allocVector(STRSXP, 1));
	SET_STRING_ELT(class, 0, mkChar(CONN_CLASS));
	classgets(connobj, class);
	INTEGER_POINTER(attr)[0] = sock;
	setAttrib(connobj, install(SOCK_ATTR), attr);
	UNPROTECT(3);
	return connobj;
}

SEXP mapiDisconnect(SEXP conn) {
	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];
	shutdown(sock, 2);
	return R_NilValue;
}

SEXP mapiRead(SEXP conn) {
	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];

	SEXP lines;
	char read_buf[BUFSIZE];
	int n, block_final, block_length;
	short header;
	size_t response_buf_len = ALLOCSIZE;
	size_t response_buf_offset = 0;

	char* response_buf = malloc(response_buf_len);
	if (response_buf == NULL) {
		error("XX: ERROR allocating memory");
	}

	block_final = FALSE;
	while (!block_final) {
		//  read block header and extract block length and final bit from header
		// this assumes little-endianness (so sue me)

		n = read(sock, (void *) &header, 2);
		if (n != 2) {
			error("XX: ERROR reading MAPI block header");
		}
		block_length = header >> 1;
		if (block_length < 0 || block_length > BLOCKSIZE) {
			error("XX: Invalid block size %i\n", block_length);
		}
		block_final = header & 1;
		n = read(sock, read_buf, block_length);
		if (n != block_length) {
			error("XX: ERROR reading block of %u bytes (final=%s) from socket",
					block_length, block_final ? "true" : "false");
		}
		if (DEBUG) {
			printf("II: Received block of %u bytes, final=%s\n", block_length,
					block_final ? "true" : "false");
		}
		read_buf[block_length] = '\0';
		// lets see whether we need moar memory for the response
		while (response_buf_offset + block_length > response_buf_len) {
			response_buf_len += ALLOCSIZE;
			if (DEBUG) {
				printf("II: Reallocating memory, new size %lu\n",
						response_buf_len);
			}
			response_buf = realloc(response_buf, response_buf_len);
			if (response_buf == NULL) {
				error("XX: ERROR allocating memory");
			}
		}
		// now that we know to have enough space in the buffer, let's copy
		memcpy(response_buf + response_buf_offset, read_buf, block_length);
		response_buf_offset += block_length;
	}

	response_buf[response_buf_offset] = '\0';
	PROTECT(lines = NEW_STRING(1));
	SET_STRING_ELT(lines, 0, mkChar(response_buf));
	free(response_buf);
	unprotect(1);
	return lines;
}

SEXP mapiWrite(SEXP conn, SEXP message) {
	assert(IS_CHARACTER(message));
	assert(GET_LENGTH(message) == 1);

	SOCKET sock = INTEGER_POINTER(
			AS_INTEGER(getAttrib(conn, install(SOCK_ATTR))))[0];
	const char *messageval = CHAR(STRING_ELT(message, 0));
	assert(strlen(messageval) > 0);

	size_t message_len = strlen(messageval);
	int n, block_final, block_length;

	size_t request_offset = 0;

	while (request_offset < message_len) {
		if (message_len - request_offset > BLOCKSIZE) {
			block_length = BLOCKSIZE;
			block_final = FALSE;
		} else {
			block_length = message_len - request_offset;
			block_final = TRUE;
		}
		if (DEBUG) {
			printf("II: Writing block of %u bytes, final=%s\n", block_length,
					block_final ? "true" : "false");
		}
		short header = (short) (block_length << 1);
		if (block_final) {
			header |= 1;
		}
		n = write(sock, (void *) &header, 2);
		if (n != 2) {
			error("XX: ERROR writing MAPI block header");
		}
		n = write(sock, messageval + request_offset, block_length);
		if (n != block_length) {
			error("XX: ERROR writing block of %u bytes (final=%s) to socket",
					block_length, block_final ? "true" : "false");
		}
		request_offset += block_length;
	}
	return R_NilValue;
}
