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
	"math/rand"
	"s3pool/conf"
	"s3pool/op"
	"time"
)

func Bucketmon() chan<- string {
	bmnotify := make(chan string, 10)

	go func() {
		tick := time.Tick(time.Second)
		bktmap := make(map[string](int))
		for {
			select {
			case bkt := <-bmnotify:
				if bkt == "" {
					// this is a special message to notify that
					// conf.RefreshInterval has changed

					// if expiration time of any bucket is beyond RefreshInterval,
					// set it to a random value between 0 and RefreshInterval
					for bkt := range bktmap {
						if bktmap[bkt] > conf.RefreshInterval*60 {
							bktmap[bkt] = rand.Intn(conf.RefreshInterval * 60)
						}
					}

					continue
				}

				// if not in bktmap, add it with an expiration of 0 ... we will
				// almost immediately refresh it.
				if _, ok := bktmap[bkt]; !ok {
					bktmap[bkt] = 0
				}
			case <-tick:
				//log.Println("tick")
				for bkt := range bktmap {
					bktmap[bkt]--
					//log.Println("bucket", bkt, bktmap[bkt])
					if bktmap[bkt] <= 0 {
						// note: maybe we should run the refresh in a
						// separate go routine?
						log.Println("BUCKETMON refresh", bkt)
						_, err := op.Refresh([]string{bkt})
						log.Println("BUCKETMON fin", bkt)
						if err != nil {
							log.Printf("WARNING: autorefresh %s failed: %v\n", bkt, err)
							delete(bktmap, bkt)
							continue
						}
						bktmap[bkt] = conf.RefreshInterval * 60
					}
				}
				//log.Println("tick done")
			}
		}
	}()

	return bmnotify
}
