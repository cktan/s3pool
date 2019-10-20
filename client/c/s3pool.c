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
#include "s3pool.h"

struct s3pool_t {
	int   port;
	char  buf[1000];
	char  errmsg[400];
};


s3pool_t* s3pool_connect(int port)
{
	s3pool_t* handle = malloc(sizeof(*handle));
	if (handle) {
		handle->port = port;
		handle->errmsg[0] = 0;
	}
	return handle;
}


void s3pool_close(s3pool_t* handle)
{
	free(handle);
}


const char* s3pool_errmsg(s3pool_t* handle)
{
	return handle->errmsg;
}


static char* chat(s3pool_t* handle, const char* request)
{
	int sockfd;
	struct sockaddr_in servaddr;

	// socket create and varification
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "socket: %s", strerror(errno));
		goto bailout;
	}
	memset(&servaddr, 0, sizeof(servaddr));
	
	// assign IP, PORT
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
	servaddr.sin_port = htons(handle->port);

	// connect the client socket to server socket
	if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "connect: %s", strerror(errno));
		goto bailout;
	}

	{
		const char* p = request;
		const char* q = request + strlen(request);
		while (p < q) {
			int n = write(sockfd, p, q-p);
			if (n == -1) {
				if (errno == EAGAIN) continue;
				
				snprintf(handle->errmsg, sizeof(handle->errmsg), "write: %s", strerror(errno));
				goto bailout;
			}
			
			p += n;
		}
	}


	{
		char* p = handle->buf;
		char* q = p + sizeof(handle->buf);
		while (p < q) {
			int n = read(sockfd, p, q-p);
			if (n == -1) {
				if (errno == EAGAIN) continue;

				snprintf(handle->errmsg, sizeof(handle->errmsg), "read: %s", strerror(errno));
				goto bailout;
			}
			if (n == 0) break;

			p += n;
		}

		if (p == q) {
			snprintf(handle->errmsg, sizeof(handle->errmsg), "read: reply message too big");
			goto bailout;
		}
	}

	close(sockfd);
	return handle->buf;

	bailout:
	if (sockfd >= 0) close(sockfd);
	return 0;
}




/**
 *  PULL a file from S3 to local disk. Returns a unix file descriptor
 *  of the file that can be used to read the file or -1 on error.
 */
int s3pool_pull(s3pool_t* handle, const char* bucket, const char* key)
{
	char* request = 0;
	char* reply = 0;
	int fd = -1;

	if (strchr(bucket, '\"') || strchr(key, '\"')) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "DQUOTE char in bucket or key");
		goto bailout;
	}

	request = malloc(strlen(bucket) + strlen(key) + 20);
	if (!request) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "out of memory");
		goto bailout;
	}
	
	sprintf(request, "[\"PULL\", \"%s\", \"%s\"]\n", bucket, key);
	reply = chat(handle, request);
	if (! reply) {
		goto bailout;
	}

	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "%s", reply + 6);
		goto bailout;
	}

	if (strncmp(reply, "OK\n", 3) != 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "bad message from server");
		goto bailout;
	}

	char* fname = reply + 3;
	char* endp = strchr(fname, '\n');
	if (endp) *endp = 0;

	fd = open(fname, O_RDONLY);
	if (fd == -1) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "open: %s", strerror(errno));
		goto bailout;
	}

	free(request);
	return fd;

	bailout:
	if (fd != -1) close(fd);
	if (request) free(request);
	return -1;
}




/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
int s3pool_push(s3pool_t* handle, const char* bucket, const char* key, const char* fpath)
{
	char* request = 0;
	char* reply = 0;

	if (strchr(bucket, '\"') || strchr(key, '\"') || strchr(fpath, '\"')) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "DQUOTE char in bucket, key or fpath");
		goto bailout;
	}

	request = malloc(strlen(bucket) + strlen(key) + strlen(fpath) + 20);
	if (!request) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "out of memory");
		goto bailout;
	}
	
	sprintf(request, "[\"PUSH\", \"%s\", \"%s\", \"%s\"]\n", bucket, key, fpath);
	reply = chat(handle, request);
	if (! reply) {
		goto bailout;
	}

	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "%s", reply + 6);
		goto bailout;
	}

	if (strncmp(reply, "OK\n", 3) != 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "bad message from server");
		goto bailout;
	}

	free(request);
	return 0;

	bailout:
	if (request) free(request);
	return -1;
}


/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
int s3pool_refresh(s3pool_t* handle, const char* bucket)
{
	char* request = 0;
	char* reply = 0;

	if (strchr(bucket, '\"')) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "DQUOTE char in bucket");
		goto bailout;
	}

	request = malloc(strlen(bucket) + 20);
	if (!request) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "out of memory");
		goto bailout;
	}
	
	sprintf(request, "[\"REFRESH\", \"%s\"]\n", bucket);
	reply = chat(handle, request);
	if (! reply) {
		goto bailout;
	}

	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "%s", reply + 6);
		goto bailout;
	}

	if (strncmp(reply, "OK\n", 3) != 0) {
		snprintf(handle->errmsg, sizeof(handle->errmsg), "bad message from server");
		goto bailout;
	}

	free(request);
	return 0;

	bailout:
	if (request) free(request);
	return -1;
}
