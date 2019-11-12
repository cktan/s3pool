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
package op

import (
	"errors"
	"strings"
)

const _MAXWORKER = 20

/**
 *  Process N items using M go routines
 */
func pmap(processitem func(n int), N int, M int) {
	// notified when a go routine is done
	// must have N reserved to avoid potential race
	fin := make(chan int, N)

	// the gate with M resources - controls #concurrent go routines at any time
	gate := make(chan int, M)
	defer close(fin)
	defer close(gate)

	// let maxworker run
	for i := 0; i < M; i++ {
		gate <- 1
	}

	// launch jobs for workers
	for i := 0; i < N; i++ {
		<-gate // wait to launch
		go func(idx int) {
			processitem(idx)
			gate <- 1  // let next guy run
			fin <- idx // notify done
		}(i)
	}

	// wait for all jobs to finish
	for i := 0; i < N; i++ {
		<-fin
	}
}

func Pull(args []string) (string, error) {
	if len(args) < 2 {
		return "", errors.New("Expected at least 2 arguments for PULL")
	}
	bucket := args[0]
	if err := checkCatalog(bucket); err != nil {
		return "", err
	}

	keys := args[1:]
	nkeys := len(keys)
	path := make([]string, nkeys)
	patherr := make([]error, nkeys)

	dowork := func(i int) {
		path[i], patherr[i] = s3GetObject(bucket, keys[i], false)
	}

	pmap(dowork, nkeys, _MAXWORKER)

	var reply strings.Builder
	for i := 0; i < nkeys; i++ {
		if patherr[i] != nil {
			return "", patherr[i]
		}
		reply.WriteString(path[i])
		reply.WriteString("\n")
	}

	return reply.String(), nil
}
