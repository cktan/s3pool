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
func Pmap(processItem func(n int), N int, M int) {
	if M > N {
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

	// let M workers run concurrently
	for i := 0; i < M; i++ {
		go func() {
			for {
				idx := <-ticket
				if idx == -1 {
					return
				}
				processItem(idx)
				fin <- idx
			}
		}()
	}

	// send the jobs. Do this in a go routine so we don't have a
	// race between ticket and fin
	go func() {
		for i := 0; i < N; i++ {
			ticket <- i
		}
	}()

	// wait for all jobs to finish
	for i := 0; i < N; i++ {
		<-fin
	}

	// Go Channel Closing Principle:
	//  - Don't close a channel from the receiver side and
	//  - Don't close a channel if the channel has multiple concurrent senders.

	// At this point, we gave out N tickets, and we received N fins.
	// All workers must be blocked waiting for ticket at this time.
	// They will next get the term signal and exit.

	// Even though we are receiver on fin, we can close it making sure
	// sure that no one will ever send to it again.
	close(fin)

	// Send the terminate signal. each worker will get the term
	// signal and MUST NOT send to fin (it was closed above).
	for i := 0; i < M; i++ {
		ticket <- -1
	}

	// we are the only one sending on ticket, so it is always safe
	// for us to close ticket.
	close(ticket)
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
