/*
 * S3pool - S3 cache on local disk
 * Copyright (c) 2019 CK Tan
 * cktanx@gmail.com
 *
 *
 * S3Pool can be used for free under the GNU General Public License
 * version 3 (where anything released into public must be open source) or
 * under a commercial license if such has been acquired (send email to
 * cktanx@gmail.com). The commercial license does not cover derived or
 * ported versions created by third parties under GPL.
 */
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
