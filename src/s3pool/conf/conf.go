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
package conf

import "time"

var VerboseLevel = 1
var RefreshInterval = 15 // in minutes
var BucketmonChannel chan<- string
var PullConcurrency = 20
var UpSince = time.Now()
var IsMaster bool
var Master string
var Standby string
var CountPull int64
var CountPullHit int64
var CountRefresh int64
var CountPush int64
var CountGlob int64

func Verbose(level int) bool {
	return VerboseLevel >= level
}

func NotifyBucketmon(bkt string) {
	BucketmonChannel <- bkt
}
