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
	"errors"
	"github.com/gobwas/glob"
	"s3pool/cat"
	"strings"
)

func Globx(args []string) (string, error) {
	var err error

	if len(args) != 2 {
		return "", errors.New("expects 2 arguments for GLOBX")
	}
	bucket, pattern := args[0], args[1]

	Refresh([]string{args[0]})

	// prepare the pattern glob
	g, err := glob.Compile(pattern, '/')
	if err != nil {
		return "", err
	}

	filter := func(key string) bool {
		return g.Match(key)
	}
	key := cat.Scan(bucket, filter)

	var replyBuilder strings.Builder
	for i := range key {
		replyBuilder.WriteString(key[i])
		replyBuilder.WriteString("\n")
	}

	return replyBuilder.String(), nil
}
