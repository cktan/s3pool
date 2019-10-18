package main

import (
	"log"
	"s3pool/op"
	"time"
)

func listmon(newbktchannel <-chan string) {
	bktmap := make(map[string](bool))
	tick := time.Tick(5 * 60 * time.Second)
	for {
		select {
		case bkt := <-newbktchannel:
			bktmap[bkt] = true
		case <-tick:
			for bkt := range bktmap {
				_, err := op.Refresh([]string{bkt})
				if err != nil {
					log.Printf("WARNING: autorefresh %s failed: %v", bkt, err)
					delete(bktmap, bkt)
				}
			}
		}
	}
}
