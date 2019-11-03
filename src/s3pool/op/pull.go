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
 *  cktan@gmail.com.
 */
package op

import (
	"errors"
	"strings"
)

const _MAXWORKER = 20

func pmap(processitem func(idx int), maxidx int, maxworker int) {
	// notified when a go routine is done
	// must have maxidx reserved to avoid potential race
	fin := make(chan int, maxidx) 

	// the gate - controls #concurrent go routines at any time
	gate := make(chan int, maxworker)
	defer close(fin)
	defer close(gate)

	for i := 0; i < maxworker; i++ {
		gate <- 1
	}
	for i := 0; i < maxidx; i++ {
		<-gate
		go func(idx int) {
			processitem(idx)
			gate <- 1
			fin <- idx
		}(i)
	}

	for i := 0; i < maxidx; i++ {
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
