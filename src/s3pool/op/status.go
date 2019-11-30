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
package op

import (
	"s3pool/conf"
	"strings"
	"fmt"
)

func Status(_ []string) (string, error) {

	var reply strings.Builder
	
	fmt.Fprintf(&reply, "count_glob %v\n", conf.CountGlob)
	fmt.Fprintf(&reply, "count_pull %v\n", conf.CountPull)
	fmt.Fprintf(&reply, "count_pull_hit %v\n", conf.CountPullHit)
	fmt.Fprintf(&reply, "count_push %v\n", conf.CountPush)
	fmt.Fprintf(&reply, "count_refresh %v\n", conf.CountRefresh)
	fmt.Fprintf(&reply, "is_master %v\n", conf.IsMaster)
	fmt.Fprintf(&reply, "master %v\n", conf.Master)
	fmt.Fprintf(&reply, "pull_concurrency %v\n", conf.PullConcurrency)
	fmt.Fprintf(&reply, "refresh_interval %v\n", conf.RefreshInterval)
	fmt.Fprintf(&reply, "revision %v\n", conf.Revision)
	fmt.Fprintf(&reply, "standby %v\n", conf.Standby)
	fmt.Fprintf(&reply, "up_since %v\n", conf.UpSince)
	fmt.Fprintf(&reply, "verbose %v\n", conf.VerboseLevel)

	return reply.String(), nil
}
