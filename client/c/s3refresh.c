/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktanx@gmail.com.
 */
#define _XOPEN_SOURCE 500
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "s3pool.h"

void usage(const char* pname, const char* msg)
{
	fprintf(stderr, "Usage: %s [-h] -p port bucket\n", pname);
	fprintf(stderr, "Refresh the __catalog__ file in an s3 bucket.\n\n");
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


void doit(int port, char* bucket)
{
	char errmsg[200];

	if (0 != s3pool_refresh(port, bucket, 
							errmsg, sizeof(errmsg))) {
		fatal(errmsg);
	}
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
		usage(argv[0], "Need bucket");
	}
	char* bucket = argv[optind++];

	if (optind != argc) {
		usage(argv[0], "Extra arguments");
	}
	
	doit(port, bucket);

	return 0;
}
