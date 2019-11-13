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
package mon

import (
	"log"
	"s3pool/op"
	"time"
)

var NotifyBucketmon chan<- string

func Bucketmon() {
	const REFRESHINTERVAL = 15 // minutes
	ch := make(chan string, 10)
	NotifyBucketmon = ch

	go func() {
		tick := time.Tick(REFRESHINTERVAL * time.Minute)
		bktmap := make(map[string](bool))
		for {
			select {
			case bkt := <-ch:
				bktmap[bkt] = true
			case <-tick:
				for bkt := range bktmap {
					log.Println("bucketmon refresh", bkt)
					_, err := op.Refresh([]string{bkt})
					if err != nil {
						log.Printf("WARNING: autorefresh %s failed: %v\n", bkt, err)
					}
				}
			}
		}
	}()
}
