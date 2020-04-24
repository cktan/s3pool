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
	"s3pool/conf"
	"s3pool/jobqueue"
	"s3pool/s3"
	"strings"
	"sync"
)

var pullQueue = jobqueue.New(conf.PullConcurrency)

func Pull(args []string) (string, error) {
	conf.CountPull++
	if len(args) < 2 {
		return "", errors.New("Expected at least 2 arguments for PULL")
	}
	bucket, keys := args[0], args[1:]

	nkeys := len(keys)
	path := make([]string, nkeys)
	patherr := make([]error, nkeys)
	waitGroup := sync.WaitGroup{}
	var hit bool

	dowork := func(i int) {
		path[i], hit, patherr[i] = s3.GetObject(bucket, keys[i], false)
		if hit {
			conf.CountPullHit++
		}
		waitGroup.Done()
	}

	// download nkeys in parallel
	waitGroup.Add(nkeys)
	for i := 0; i < nkeys; i++ {
		pullQueue.Add(dowork, i)
	}
	waitGroup.Wait()

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
