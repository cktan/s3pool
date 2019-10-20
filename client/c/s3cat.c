#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "s3pool.h"

void usage(const char* pname, const char* msg)
{
	fprintf(stderr, "Usage: %s [-h] -p port bucket:key ...\n", pname);
	fprintf(stderr, "Copy s3 files to stdout.\n\n");
	fprintf(stderr, "    -p port : specify the port number of s3pool process\n");
	fprintf(stderr, "    -h      : print this help message\n");
	fprintf(stderr, "\n");
	if (msg) {
		fprintf(stderr, "%s\n\n", msg);
	}

	exit(msg ? -1 : 0);
}


void fatal(const char* msg)
{
	fprintf(stderr, "FATAL: %s\n", msg);
	exit(1);
}


void doit(int port, char* bktkey_)
{
	char* bktkey = strdup(bktkey_);
	if (!bktkey) {
		fatal("out of memory");
	}

	char* colon = strchr(bktkey, ':');
	if (!colon) {
		fatal("missing colon char in bucket:key");
	}

	*colon = 0;
	char* bucket = bktkey;
	char* key = colon+1;

	s3pool_t* handle = s3pool_connect(port);
	if (!handle) {
		fatal(s3pool_errmsg(handle));
	}

	int fd = s3pool_pull(handle, bucket, key);
	if (-1 == fd) {
		fatal(s3pool_errmsg(handle));
	}

	while (1) {
		char buf[100];
		int n = read(fd, buf, sizeof(buf));
		if (n == 0) break;
		if (n == -1) {
			perror("read");
			exit(1);
		}

		if (1 != fwrite(buf, n, 1, stdout)) {
			perror("fwrite");
			exit(1);
		}
	}

	close(fd);
	s3pool_close(handle);
	free(bktkey);
	
}




int main(int argc, char* argv[])
{
	int opt;
	int port = -1;
	while ((opt = getopt(argc, argv, "p:h")) != -1) {
		switch (opt) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'h':
			usage(argv[0], 0);
			break;
		default:
			usage(argv[0], "Bad command line flag");
			break;
		}
	}

	if (! (0 < port && port <= 65535)) {
		usage(argv[0], "Bad or missing port number");
	}

	if (optind >= argc) {
		usage(argv[0], "Need bucket and key");
	}
	
	for (int i = optind; i < argc; i++) {
		doit(port, argv[i]);
	}

	return 0;
}
