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
/**
 *  Process N items using M go routines
 */
func Pmap(processItem func(n int), N int, M int) {
	if (M > N) {
		M = N
	}

	var fin chan int
	var ticket chan int
	if false {
		// for debug
		fin = make(chan int)
		ticket = make(chan int)
	} else {
		fin = make(chan int, 10)
		ticket = make(chan int, 10)
	} 
	defer func() {
		close(fin)
		close(ticket)
	}()

	// let M go routines run concurrently
	for i := 0; i < M; i++ {
		go func() {
			for {
				idx := <- ticket
				if idx == -1 {
					return
				}
				processItem(idx)
				fin <- idx
			}
		}()
	}

	go func() {
		// send the jobs
		for i := 0; i < N; i++ {
			ticket <- i
		}
		// send the terminate signal
		for i := 0; i < M; i++ {
			ticket <- -1
		}
	}()
	
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
