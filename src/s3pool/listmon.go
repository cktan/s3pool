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
 *  cktan@gmail.com.
 */
package main

import (
	"log"
	"s3pool/op"
	"time"
)


func listmon(newbktchannel <-chan string) {
	bktmap := make(map[string](bool))
	tick := time.Tick(5 * 60 * time.Second) // 5 minute tick
	for {
		select {
		case bkt := <-newbktchannel:
			if _, ok := bktmap[bkt]; !ok {
				_, err := op.Refresh([]string{bkt})
				if err != nil {
					log.Printf("WARNING: autorefresh %s failed: %v", bkt, err)
					continue
				}
				bktmap[bkt] = true
			}
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
