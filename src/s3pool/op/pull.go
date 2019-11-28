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
	"s3pool/s3"
	"strings"
	"sync"
)

const _MAXWORKER = 20

/**
 *  Process N items using M go routines
 */
func Pmap(processItem func(n int), N int, M int) {
	if M > N {
		M = N
	}

	wg := &sync.WaitGroup{}
	ticket := make(chan int, 10)

	// let M workers run concurrently
	wg.Add(M)
	for i := 0; i < M; i++ {
		go func() {
			for idx := range ticket {
				processItem(idx)
			}
			wg.Done()
		}()
	}

	// send the jobs
	for i := 0; i < N; i++ {
		ticket <- i
	}
	close(ticket)

	// wait for all jobs to finish
	wg.Wait()
}

func Pull(args []string) (string, error) {
	if len(args) < 2 {
		return "", errors.New("Expected at least 2 arguments for PULL")
	}
	bucket, keys := args[0], args[1:]
	if err := checkCatalog(bucket); err != nil {
		return "", err
	}

	nkeys := len(keys)
	path := make([]string, nkeys)
	patherr := make([]error, nkeys)

	dowork := func(i int) {
		path[i], patherr[i] = s3.GetObject(bucket, keys[i], false)
	}

	Pmap(dowork, nkeys, _MAXWORKER)

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
