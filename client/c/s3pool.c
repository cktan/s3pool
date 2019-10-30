#define _XOPEN_SOURCE 500
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include "s3pool.h"

static char* mkrequest(int argc, const char** argv, char* errmsg, int errmsgsz)
{
	int len = 4;				/* for [ ] \n \0 */
	int i;
	char* request = 0;

	for (i = 0; i < argc; i++) {
		// for each arg X, we want to make 'X', - quote quote comma space
		// so, reserve extra space for those chars here
		len += strlen(argv[i]) + 4; 
		if (strchr(argv[i], '\"')) {
			snprintf(errmsg, errmsgsz, "DQUOTE char not allowed");
			goto bailout;
		}
		if (strchr(argv[i], '\n')) {
			snprintf(errmsg, errmsgsz, "NEWLINE char not allowed");
			goto bailout;
		}
	}
	request = malloc(len);
	if (! request) {
		snprintf(errmsg, errmsgsz, "out of memory");
		goto bailout;
	}

	char* p = request;
	*p++ = '[';
	for (i = 0; i < argc; i++) {
		sprintf(p, "\"%s\"%s", argv[i], i < argc - 1 ? "," : "");
		p += strlen(p);
	}
	*p++ = ']';
	*p++ = '\n';
	*p = 0;						/* NUL */

	assert((int)strlen(p) + 1 <= len);
	return request;

	bailout:
	if (request) free(request);
	return 0;
}

static int send_request(int sockfd, const char* request,
						char* errmsg, int errmsgsz)
{
	const char* p = request;
	const char* q = request + strlen(request);
	while (p < q) {
		int n = write(sockfd, p, q-p);
		if (n == -1) {
			if (errno == EAGAIN) continue;
			
			snprintf(errmsg, errmsgsz, "write: %s", strerror(errno));
			return -1;
		}
			
		p += n;
	}
	return 0;
}



static int check_reply(char* reply, char* errmsg, int errmsgsz)
{
	if (strncmp(reply, "OK\n", 3) == 0) {
		return 0;
	}
	
	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(errmsg, errmsgsz, "%s", reply + 6);
		return -1;
	}

	snprintf(errmsg, errmsgsz, "bad message from server");
	return -1;
}





static char* chat(int port, const char* request,
				  char* errmsg, int errmsgsz)
{
	int sockfd = -1;
	struct sockaddr_in servaddr;
	int replysz = 0;
	char* reply = 0;

	// socket create and verification
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1) {
		snprintf(errmsg, errmsgsz, "socket: %s", strerror(errno));
		goto bailout;
	}
	memset(&servaddr, 0, sizeof(servaddr));
	
	// assign IP, PORT
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
	servaddr.sin_port = htons(port);

	// connect the client socket to server socket
	if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0) {
		snprintf(errmsg, errmsgsz, "connect: %s", strerror(errno));
		goto bailout;
	}

	// send the request
	if (-1 == send_request(sockfd, request, errmsg, errmsgsz)) {
		goto bailout;
	}

	// read the reply
	char* p = reply;
	char* q = reply + replysz;
	while (1) {
		// always keep one extra byte slack for NUL term
		if (p + 1 >= q) {
			int newsz = replysz * 1.5;
			if (newsz == 0) newsz = 1024;
			char* t = realloc(reply, newsz);
			if (!t) {
				snprintf(errmsg, errmsgsz, "read: reply message too big -- out of memory");
				goto bailout;
			}
			p = t + (p - reply);
			q = t + newsz;
			reply = t;
			replysz = newsz;
		}

		assert(p + 1 < q);
		int n = read(sockfd, p, q - p - 1);
		if (n == -1) {
			if (errno == EAGAIN) continue;

			snprintf(errmsg, errmsgsz, "read: %s", strerror(errno));
			goto bailout;
		}
		if (n == 0) break;

		p += n;
		*p = 0;					/* NUL */
	}

	close(sockfd);

	if (-1 == check_reply(reply, errmsg, errmsgsz)) {
		goto bailout;
	}

	char* aptr = strdup(reply+3);
	if (!aptr) {
		snprintf(errmsg, errmsgsz, "%s", "out of memory");
		goto bailout;
	}

	free(reply);
	return aptr;

	bailout:
	if (sockfd >= 0) close(sockfd);
	if (reply) free(reply);
	return 0;
}



/**

   PULL a file from S3 to local disk. 
 
   On success, return the path to the file pulled down from S3. Caller
   must free() the pointer returned. 
 
   On failure, return a NULL ptr.

 */
char* s3pool_pull_ex(int port, const char* bucket,
					 const char* key[], int nkey,
					 char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	int fd = -1;
	const char* argv[2+nkey];

	argv[0] = "PULL";
	argv[1] = bucket;
	for (int i = 0; i < nkey; i++)
		argv[i+2] = key[i];
	
	request = mkrequest(2+nkey, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}


	free(request);
	return reply;

	bailout:
	if (fd != -1) close(fd);
	if (request) free(request);
	if (reply) free(reply);
	return 0;
}

char* s3pool_pull(int port, const char* bucket, const char* key,
				  char* errmsg, int errmsgsz)
{
	char* reply = s3pool_pull_ex(port, bucket, &key, 1, errmsg, errmsgsz);
	char* term = strchr(reply, '\n');
	if (term) *term = 0;
	return reply;
}



/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
int s3pool_push(int port, const char* bucket, const char* key, const char* fpath,
				char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[4] = { "PUSH", bucket, key, fpath };

	request = mkrequest(4, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	free(reply);
	return 0;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return -1;
}


/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
int s3pool_refresh(int port, const char* bucket,
				   char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[2] = { "REFRESH", bucket };

	request = mkrequest(2, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	free(reply);
	return 0;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return -1;
}

/**

   GLOB file names in a bucket. 
 
   On success, return a buffer containing strings terminated by
   NEWLINE. Each string is a path name in the S3 bucket that matched
   pattern. Caller must free() the buffer returned.
 
   On failure, return a NULL ptr.

*/
char* s3pool_glob(int port, const char* bucket, const char* pattern,
				  char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[3] = {"GLOB", bucket, pattern};

	request = mkrequest(3, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}
	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	return reply;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return 0;
}

