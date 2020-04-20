package s3meta

import (
	"strings"
	"sync"
)

type storeCB struct {
	sync.RWMutex

	prefix []string
	key    map[string]([]string) // prefix -> keys
	etag   map[string]string     // key -> etag
}

var storeLock = sync.Mutex{}
var storeList = make(map[string]*storeCB)

func invalidate(bucket string) {
	storeLock.Lock()
	delete(storeList, bucket)
	storeLock.Unlock()
}

func getKnownBuckets() []string {
	storeLock.Lock()
	list := make([]string, len(storeList))
	i := 0
	for k := range storeList {
		list[i] = k
		i++
	}
	storeLock.Unlock()
	return list
}

func getStore(bucket string) *storeCB {
	storeLock.Lock()
	x := storeList[bucket]
	if x == nil {
		x = newStore()
		storeList[bucket] = x
	}
	storeLock.Unlock()
	return x
}

func newStore() *storeCB {
	var p storeCB

	p.prefix = make([]string, 0, 10)
	p.key = make(map[string]([]string))
	p.etag = make(map[string]string)
	return &p
}

func bisectLeft(arr []string, x string) int {
	// see https://github.com/python/cpython/blob/2.7/Lib/bisect.py
	lo := 0
	hi := len(arr)
	for lo < hi {
		mid := (lo + hi) / 2
		if arr[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (p *storeCB) setETag(key string, etag string) {
	p.etag[key] = etag
}

func (p *storeCB) getETag(key string) string {
	return p.etag[key]
}

func (p *storeCB) remove(prefix string) {
	p.Lock()
	idx := bisectLeft(p.prefix, prefix)
	if idx < len(p.prefix) && p.prefix[idx] == prefix {
		// delete prefix from p.prefix[]
		a := p.prefix
		a = append(a[:idx], a[idx+1:]...)
		p.prefix = a

		// delete all etags of keys belonging to prefix
		for _, k := range p.key[prefix] {
			delete(p.etag, k)
		}

		// delete all keys of prefix
		delete(p.key, prefix)
	}
	p.Unlock()
}

func (p *storeCB) insert(prefix string, key, etag []string) {

	if len(key) != len(etag) {
		panic("len key != len etag")
	}

	p.Lock()
	idx := bisectLeft(p.prefix, prefix)
	if idx < len(p.prefix) && p.prefix[idx] == prefix {
		p.Unlock()
		p.remove(prefix)
		p.Lock()
		idx = bisectLeft(p.prefix, prefix)
	}

	// insert prefix at p.prefix[idx]
	a := make([]string, 0, len(p.prefix)+1)
	a = append(a, p.prefix[:idx]...)
	a = append(a, prefix)
	a = append(a, p.prefix[idx:]...)
	p.prefix = a

	// make a copy of key[] and save it
	p.key[prefix] = make([]string, len(key))
	copy(p.key[prefix], key)

	// for each key, save its corresponding etag
	for i, k := range key {
		p.etag[k] = etag[i]
	}
	p.Unlock()
}

func filter(a []string, test func(string) bool) (ret []string) {
	for _, s := range a {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func (p *storeCB) retrieve(prefix string) (key []string, etag []string, ok bool) {
	p.RLock()

	kk := p.key[prefix]
	if kk == nil {
		// if we have [/A, /B, /C] in existing prefix, then
		// searching for /A/X/Y should match /A
		idx := bisectLeft(p.prefix, prefix)
		idx--
		if 0 <= idx && idx < len(p.prefix) {
			if strings.HasPrefix(prefix, p.prefix[idx]) {
				kk = filter(p.key[p.prefix[idx]], func(s string) bool {
					return strings.HasPrefix(s, prefix)
				})
			}
		}
	}

	if ok = (kk != nil); ok {
		for _, k := range kk {
			key = append(key, k)
			etag = append(etag, p.etag[k])
		}
	}

	p.RUnlock()
	return
}
