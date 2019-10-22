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
 *  cktan@gmail.com.
 */
package op

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"github.com/gobwas/glob"
	"bufio"
)



func Glob(args []string) (string, error) {
	if len(args) != 2 {
		return "", errors.New("expects 2 arguments for GLOB")
	}
	bucket, pattern := args[0], args[1]

	g, err := glob.Compile(pattern, '/')
	if err != nil {
		return "", err
	}
	
	// Open the file. Retry after s3ListObjects() if it does not exist.
	file, err := os.Open(fmt.Sprintf("data/%s/__list__", bucket))
	if (err != nil) {
		if os.IsNotExist(err) {
			if err = s3ListObjects(bucket); err == nil {
				file, err = os.Open(fmt.Sprintf("data/%s/__list__", bucket))
			}
		}
		if (err != nil) {
			return "", err
		}
	}
	defer file.Close()

	// Match the pattern against content of the __list__ file
	var replyBuilder strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		matched := g.Match(line)
		if matched {
			replyBuilder.WriteString(line)
			replyBuilder.WriteString("\n")
		}
	}

	if err = scanner.Err(); err != nil {
		return "", err
	}

	return replyBuilder.String(), nil
}
