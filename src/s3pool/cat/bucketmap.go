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
package cat

import (
	"sync"
)

type BucketMap struct {
	sync.RWMutex
	Map map[string]*KeyMap
}

func newBucketMap() *BucketMap {
	return &BucketMap{Map: make(map[string]*KeyMap)}
}

func (bm *BucketMap) Keys() []string {
	res := make([]string, 0, 10)
	bm.RLock()
	for k := range bm.Map {
		res = append(res, k)
	}
	bm.RUnlock()
	return res
}

func (bm *BucketMap) Get(bucket string) *KeyMap {
	bm.RLock()
	result, ok := bm.Map[bucket]
	bm.RUnlock()
	if !ok {
		return nil
	}
	return result
}

func (bm *BucketMap) Put(bucket string, kmap *KeyMap) {
	bm.Lock()
	bm.Map[bucket] = kmap
	bm.Unlock()
}

func (bm *BucketMap) Delete(bucket string) {
	bm.Lock()
	delete(bm.Map, bucket)
	bm.Unlock()
}
