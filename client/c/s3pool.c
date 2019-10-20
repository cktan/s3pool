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


static char* chat(int port, const char* request,
				  char* errmsg, int errmsgsz)
{
	int sockfd = -1;
	struct sockaddr_in servaddr;
	int replysz = 0;
	char* reply = 0;

	// socket create and varification
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

	{
		const char* p = request;
		const char* q = request + strlen(request);
		while (p < q) {
			int n = write(sockfd, p, q-p);
			if (n == -1) {
				if (errno == EAGAIN) continue;
				
				snprintf(errmsg, errmsgsz, "write: %s", strerror(errno));
				goto bailout;
			}
			
			p += n;
		}
	}


	char* p = reply;
	char* q = p + replysz;
	while (p < q) {
		if (p == q) {
			int newsz = replysz * 1.5;
			if (newsz == 0) newsz = 1024;
			char* t = realloc(reply, newsz);
			if (!t) {
				snprintf(errmsg, errmsgsz, "read: reply message too big -- out of memory");
				goto bailout;
			}
			p = t + replysz;
			q = t + newsz;
			reply = t;
			replysz = newsz;
		}

		int n = read(sockfd, p, q-p);
		if (n == -1) {
			if (errno == EAGAIN) continue;

			snprintf(errmsg, errmsgsz, "read: %s", strerror(errno));
			goto bailout;
		}
		if (n == 0) break;

		p += n;
	}

	close(sockfd);
	return reply;

	bailout:
	if (sockfd >= 0) close(sockfd);
	if (reply) free(reply);
	return 0;
}




/**
 *  PULL a file from S3 to local disk. Returns a unix file descriptor
 *  of the file that can be used to read the file or -1 on error.
 */
int s3pool_pull(int port, const char* bucket, const char* key,
				char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	int fd = -1;

	if (strchr(bucket, '\"') || strchr(key, '\"')) {
		snprintf(errmsg, errmsgsz, "DQUOTE char in bucket or key");
		goto bailout;
	}

	request = malloc(strlen(bucket) + strlen(key) + 20);
	if (!request) {
		snprintf(errmsg, errmsgsz, "out of memory");
		goto bailout;
	}

	while (1) {
		sprintf(request, "[\"PULL\", \"%s\", \"%s\"]\n", bucket, key);
		reply = chat(port, request, errmsg, errmsgsz);
		if (! reply) {
			goto bailout;
		}
		
		if (strncmp(reply, "ERROR\n", 6) == 0) {
			snprintf(errmsg, errmsgsz, "%s", reply + 6);
			goto bailout;
		}
		
		if (strncmp(reply, "OK\n", 3) != 0) {
			snprintf(errmsg, errmsgsz, "bad message from server");
			goto bailout;
		}
		
		char* fname = reply + 3;
		char* endp = strchr(fname, '\n');
		if (endp) *endp = 0;
		
		fd = open(fname, O_RDONLY);
		if (fd == -1) {
			/* special case to handle race: file may be deleted 
			 * by others right after we pulled. In this case,
			 * just pull again.
			 */
			if (errno == ENOENT) {
				continue;
			}
			snprintf(errmsg, errmsgsz, "open: %s", strerror(errno));
			goto bailout;
		}

		break;
	}

	free(request);
	free(reply);
	return fd;

	bailout:
	if (fd != -1) close(fd);
	if (request) free(request);
	if (reply) free(reply);
	return -1;
}




/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
int s3pool_push(int port, const char* bucket, const char* key, const char* fpath,
				char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;

	if (strchr(bucket, '\"') || strchr(key, '\"') || strchr(fpath, '\"')) {
		snprintf(errmsg, errmsgsz, "DQUOTE char in bucket, key or fpath");
		goto bailout;
	}

	request = malloc(strlen(bucket) + strlen(key) + strlen(fpath) + 20);
	if (!request) {
		snprintf(errmsg, errmsgsz, "out of memory");
		goto bailout;
	}
	
	sprintf(request, "[\"PUSH\", \"%s\", \"%s\", \"%s\"]\n", bucket, key, fpath);
	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(errmsg, errmsgsz, "%s", reply + 6);
		goto bailout;
	}

	if (strncmp(reply, "OK\n", 3) != 0) {
		snprintf(errmsg, errmsgsz, "bad message from server");
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

	if (strchr(bucket, '\"')) {
		snprintf(errmsg, errmsgsz, "DQUOTE char in bucket");
		goto bailout;
	}

	request = malloc(strlen(bucket) + 20);
	if (!request) {
		snprintf(errmsg, errmsgsz, "out of memory");
		goto bailout;
	}
	
	sprintf(request, "[\"REFRESH\", \"%s\"]\n", bucket);
	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(errmsg, errmsgsz, "%s", reply + 6);
		goto bailout;
	}

	if (strncmp(reply, "OK\n", 3) != 0) {
		snprintf(errmsg, errmsgsz, "bad message from server");
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
