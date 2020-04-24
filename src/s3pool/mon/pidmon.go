/**
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019-2020 CK Tan
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
	"os"
	"s3pool/pidfile"
	"time"
)

func Pidmon() {
	go func() {
		for {
			pid := pidfile.Read()
			if pid != os.Getpid() {
				log.Println("pidfile has changed. s3pool exiting ...")
				os.Exit(0)
			}
			time.Sleep(60 * time.Second)
		}
	}()
}
