package s3meta

import (
	"sync"
)

type cacheRec struct {
	query string
	reply *replyType
	dead  bool
}


type storeCB struct {
	sync.RWMutex
	cache map[string]*cacheRec    // query -> cachereq
	inverted map[string]([]*cacheRec) // s3 key -> query
}

func newStore() *storeCB {
	var p storeCB
	p.cache = make(map[string]*cacheRec)
	p.inverted = make(map[string]([]*cacheRec))
	return &p
}

func (p *storeCB) find(query string) *replyType {
	p.RLock()
	rec := p.cache[query]
	p.RUnlock()
	if rec == nil || rec.dead {
		return nil
	}
	return rec.reply
}


func (p *storeCB) insert(query string, reply *replyType) {
	rec := &cacheRec{query:query, reply:reply}

	p.Lock()
	p.cache[query] = rec
	for _, k := range reply.key {
		p.inverted[k] = append(p.inverted[k], rec)
	}
	p.Unlock()
}

func (p *storeCB) remove(query string) {
	p.Lock()
	if rec, ok := p.cache[query]; ok {
		rec.dead = true
		delete(p.cache, query)
	}
	p.Unlock()
}


func (p *storeCB) invalidate(key string) {
	p.Lock()
	arr, ok := p.inverted[key]
	if ok {
		delete(p.inverted, key)
		for _, rec := range arr {
			for i := range rec.reply.key {
				if rec.reply.key[i] == key {
					rec.reply.etag[i] = ""
				}
			}
		}
	}
	p.Unlock()
}

