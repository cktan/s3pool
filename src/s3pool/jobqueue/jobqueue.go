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
	concurrency int
	backlog     chan *Item
	waitGroup   sync.WaitGroup
}

func New(concurrency int) *JobQueue {
	if concurrency > 100 {
		// be reasonable
		concurrency = 100
	}
	jq := &JobQueue{
		concurrency: concurrency,
		backlog:     make(chan *Item, 100),
	}
	for i := 0; i < concurrency; i++ {
		jq.spawnOne()
	}
	return jq
}

func (jq *JobQueue) Destroy() {
	close(jq.backlog)
	jq.waitGroup.Wait()
}

func (jq *JobQueue) spawnOne() {
	jq.waitGroup.Add(1)
	go func() {
		for item := range jq.backlog {
			item.processItem(item.idx)
		}
		jq.waitGroup.Done()
	}()
}

func (jq *JobQueue) Add(processItem func(idx int), idx int) {
	item := &Item{idx, processItem}
	jq.backlog <- item
}
