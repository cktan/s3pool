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
	"time"
	"sync"
)

type mastermoncb struct {
	sync.RWMutex
	master, standby string
}

var mastermon = &mastermoncb{}

func Get() (master, standby string) {
	mastermon.RLock()
	master, standby = mastermon.master, mastermon.standby
	mastermon.RUnlock()
	return
}


func Set(master, standby string) {
	mastermon.Lock()
	mastermon.master, mastermon.standby = master, standby
	mastermon.Unlock()
}



func Mastermon(master, standby string) {
	for {
		Set(master, standby)
		time.Sleep(60 * time.Second)
	}
}
