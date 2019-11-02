package cat

import (
	"sync"
)

type KeyMap struct {
	sync.RWMutex
	Map map[string]string // key to etag
}

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

func (bm *BucketMap) Get(bucket string) (result *KeyMap, ok bool) {
	bm.RLock()
	result, ok = bm.Map[bucket]
	bm.RUnlock()
	return
}

func (bm *BucketMap) Put(bucket string, keymap *KeyMap) {
	bm.Lock()
	bm.Map[bucket] = keymap
	bm.Unlock()
}

func (bm *BucketMap) Delete(bucket string) {
	bm.Lock()
	delete(bm.Map, bucket)
	bm.Unlock()
}
