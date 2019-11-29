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
	"sync/atomic"
)

type Item struct {
	idx         int
	processItem func(idx int)
}

type JobQueue struct {
	nworker   int32
	backlog   chan *Item
	waitGroup sync.WaitGroup
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
	return int(jq.nworker)
}

func (jq *JobQueue) SetNWorker(n int) {
	if n < 0 {
		return
	}
	N := int32(n)
	for {
		k := atomic.LoadInt32(&jq.nworker)
		if k == N {
			break
		}
		if k < N {
			jq.addWorker()
		} else {
			jq.dropWorker()
		}
	}
}

func (jq *JobQueue) addWorker() {
	jq.waitGroup.Add(1)
	id := atomic.AddInt32(&jq.nworker, 1)
	go jq.run(id)
}

func (jq *JobQueue) dropWorker() {
	n := atomic.LoadInt32(&jq.nworker) - 1
	if n >= 0 {
		atomic.StoreInt32(&jq.nworker, n)
	}
}

func (jq *JobQueue) run(id int32) {
	for item := range jq.backlog {
		item.processItem(item.idx)
		n := atomic.LoadInt32(&jq.nworker)
		if id > n {
			// i am no longer needed
			break
		}
	}
	jq.waitGroup.Done()
}

func (jq *JobQueue) Add(processItem func(idx int), idx int) {
	item := &Item{idx, processItem}
	jq.backlog <- item
}
