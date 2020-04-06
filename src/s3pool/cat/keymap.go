package cat

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

type ItemRec struct {
	Key string
	ETag string
}


type KeyMap struct {
	sync.RWMutex
	err error
	Item []ItemRec
}


func NewKeyMap(key []string, etag []string, err error) (km *KeyMap, reterr error) {
	if (len(key) != len(etag)) {
		reterr = errors.New("len(key) != len(etag)")
		return
	}

	n := len(key)
	item := make([]ItemRec, n)
	for i := 0; i < n; i++ {
		item[i] = ItemRec{key[i], etag[i]}
	}

	sort.SliceStable(item, func(i,j int) bool { return item[i].Key < item[j].Key })

	km = &KeyMap{err: err, Item: item}
	return
}


func (p *KeyMap) bisect_right(x string) int {
	lo := 0
	hi := len(p.Item)
	a := p.Item
	for lo < hi {
		mid := (lo + hi) / 2
		if x < a[mid].Key {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (p *KeyMap) bisect_left(x string) int {
	// see https://github.com/python/cpython/blob/2.7/Lib/bisect.py
	lo := 0
	hi := len(p.Item)
	a := p.Item
	for lo < hi {
		mid := (lo + hi) / 2
		if a[mid].Key < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (p *KeyMap) SearchPrefix(prefix string) []ItemRec {
	p.RLock()
	idx := p.bisect_left(prefix)
	count := 0
	for i := idx; i < len(p.Item); i++ {
		if !strings.HasPrefix(p.Item[i].Key, prefix) {
			break
		}
	}
	ret := p.Item[idx:idx+count]
	p.RUnlock()
	return ret 
}


func (p *KeyMap) SearchExact(key string) (etag string) {
	p.RLock()
	idx := p.bisect_left(key)
	if idx < len(p.Item) && p.Item[idx].Key == key {	
		etag = p.Item[idx].ETag
	}
	p.RUnlock()
	return 
}


func (p *KeyMap) Delete(key string) {
	p.Update(key, "")
}


func (p *KeyMap) Update(key string, etag string) bool {
	ok := false
	p.RLock()			// rlock is sufficient!
	idx := p.bisect_left(key)
	if idx < len(p.Item) && p.Item[idx].Key == key {
		p.Item[idx].ETag = etag
		ok = true
	}
	p.RUnlock()
	return ok
}


func (p *KeyMap) Upsert(key string, etag string) {
	p.Lock()
	idx := p.bisect_right(key)
	if idx == len(p.Item) {
		p.Item = append(p.Item, ItemRec{key, etag})
	} else if (p.Item[idx].Key == key) {
		p.Item[idx].ETag = etag
	} else {
		x := make([]ItemRec, 0, len(p.Item) + 1)
		x = append(p.Item[:idx], ItemRec{key, etag})
		x = append(x, p.Item[idx:]...)
		p.Item = x
	}
	p.Unlock()
}
