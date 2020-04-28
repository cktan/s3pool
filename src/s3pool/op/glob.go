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
package op

import (
	"errors"
	"github.com/cktan/glob"
	"s3pool/conf"
	"s3pool/mop"
	"strings"
)

func globPrefix(pattern string) string {
	s := pattern
	s = strings.SplitN(s, "*", 2)[0]
	s = strings.SplitN(s, "?", 2)[0]
	return s
}

func Glob(args []string) (string, error) {
	conf.CountGlob++

	var err error

	if len(args) != 2 {
		return "", errors.New("expects 2 arguments for GLOB")
	}
	bucket, pattern := args[0], args[1]

	// prepare the pattern glob
	g, err := glob.Compile(pattern, '/')
	if err != nil {
		return "", err
	}

	filter := func(key string) bool {
		return g.Match(key)
	}

	// get list of potential matches
	prefix := globPrefix(pattern)
	key, err := mop.ListPrefix(bucket, prefix)

	// filter the matches; return only those that matched
	var replyBuilder strings.Builder
	for _, k := range key {
		if filter(k) {
			replyBuilder.WriteString(k)
			replyBuilder.WriteString("\n")
		}
	}

	return replyBuilder.String(), nil
}
