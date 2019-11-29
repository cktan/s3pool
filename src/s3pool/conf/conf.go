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

var VerboseLevel = 1
var RefreshInterval = 15 // in minutes
var BucketmonChannel chan<- string
var PullConcurrency = 20

func Verbose(level int) bool {
	return VerboseLevel >= level
}

func NotifyBucketmon(bkt string) {
	BucketmonChannel <- bkt
}
