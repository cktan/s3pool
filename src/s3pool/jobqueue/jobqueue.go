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
package jobqueue

import (
	"sync"
)

type Item struct {
	idx         int
	processItem func(idx int)
}

type JobQueue struct {
	sync.Mutex                // protects nworker and nzombie
	nworker    int            // # go routines running
	nzombie    int            // # those dying
	backlog    chan *Item     // send jobs through this channel
	waitGroup  sync.WaitGroup // sync for group exit
}

func New(nworker int) *JobQueue {
	jq := &JobQueue{backlog: make(chan *Item, 100)}
	jq.SetNWorker(nworker)
	return jq
}

func (jq *JobQueue) Destroy() {
	close(jq.backlog)
	jq.waitGroup.Wait()
}

func (jq *JobQueue) NWorker() int {
	return jq.nworker
}

func (jq *JobQueue) SetNWorker(n int) {
	if n < 0 {
		return
	}

	addition := 0
	jq.Lock()
	if true {
		// increase workers
		for jq.nworker-jq.nzombie < n {
			if jq.nzombie > 0 {
				jq.nzombie--
			} else {
				jq.nworker++
				addition++
			}
		}

		// decrease workers
		for jq.nworker-jq.nzombie > n {
			jq.nzombie++
		}
	}
	jq.Unlock()

	for i := 0; i < addition; i++ {
		jq.waitGroup.Add(1)
		go jq.run()
	}
}

func (jq *JobQueue) run() {
	for item := range jq.backlog {
		item.processItem(item.idx)
		if jq.nzombie > 0 {
			exit := false
			jq.Lock()
			if jq.nzombie > 0 {
				jq.nzombie--
				exit = true
			}
			jq.Unlock()

			if exit {
				break
			}
		}
	}
	jq.waitGroup.Done()
}

func (jq *JobQueue) Add(processItem func(idx int), idx int) {
	item := &Item{idx, processItem}
	jq.backlog <- item
}
