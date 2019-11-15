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
	"s3pool/conf"
	"s3pool/op"
	"time"
)

var bucketmonCh chan<- string

func NotifyBucketmon(bucket string) {
	bucketmonCh <- bucket
}

func Bucketmon() {
	ch := make(chan string, 10)
	bucketmonCh = ch

	go func() {
		countdown := conf.RefreshInterval
		tick := time.Tick(time.Minute)
		bktmap := make(map[string](bool))
		for {
			select {
			case bkt := <-ch:
				log.Printf("Bucket notify %s\n", bkt)
				bktmap[bkt] = true
			case <-tick:
				log.Printf("BUCKETMON %d", countdown)
				countdown--
				if countdown > 0 {
					continue
				}
				countdown = conf.RefreshInterval
				for bkt := range bktmap {
					log.Println("BUCKETMON refresh", bkt)
					_, err := op.Refresh([]string{bkt})
					if err != nil {
						log.Printf("WARNING: autorefresh %s failed: %v\n", bkt, err)
						delete(bktmap, bkt)
					}
					log.Println("BUCKETMON fin", bkt)
				}
			}
		}
	}()
}
