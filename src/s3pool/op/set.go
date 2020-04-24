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
	"s3pool/conf"
	"strconv"
	"strings"
)

func strtobool(s string) bool {
	return s == "true" || s == "1" || s == "on"
}

func Set(args []string) (string, error) {
	if len(args) != 2 {
		return "", errors.New("expects 2 arguments for SET")
	}
	varname, varvalue := strings.ToLower(args[0]), strings.ToLower(args[1])

	if varname == "verbose" {
		i, err := strconv.Atoi(varvalue)
		if err != nil {
			return "", err
		}
		if i < 0 {
			i = 0 // minimum
		}
		conf.VerboseLevel = i
		return "\n", nil
	}

	if varname == "refresh_interval" {
		i, err := strconv.Atoi(varvalue)
		if err != nil {
			return "", err
		}
		if i < 2 {
			i = 2 // minimum
		}
		conf.RefreshInterval = i
		return "\n", nil
	}

	if varname == "pull_concurrency" {
		i, err := strconv.Atoi(varvalue)
		if err != nil {
			return "", err
		}
		if i < 5 {
			i = 5 // minimum
		}
		conf.PullConcurrency = i
		pullQueue.SetNWorker(i)
		return "\n", nil
	}

	return "", errors.New("Unknown var name")
}
