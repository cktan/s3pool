package strlock

import (
	"sync"
)

var tabmux sync.Mutex
var tabcond *sync.Cond
var tab = map[string]bool{}

func init() {
	tabcond = sync.NewCond(&tabmux)
}

func Lock(s string) (*string, error) {
	tabmux.Lock()
	defer tabmux.Unlock()
	for tab[s] {
		tabcond.Wait()
	}
	tab[s] = true
	return &s, nil
}

func Unlock(s *string) {
	tabmux.Lock()
	defer tabmux.Unlock()
	delete(tab, *s)
}
