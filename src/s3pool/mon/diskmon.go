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
package mon

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Only count utilization under data directory
func diskUsageOfSubdirs() int64 {
	cmd := exec.Command("du", "-s", "data", "-B", "1048576")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	fields := strings.Fields(string(out))
	used, err := strconv.Atoi(fields[0])
	if err != nil {
		log.Fatal(err)
	}
	return int64(used) * 1048576
}

// Instead of the disk size, we consider total available disk as what
// we currently used + what is available.
func diskUsage() (used, total int64, pct int) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(".", &fs)
	if err != nil {
		log.Fatal(err)
	}
	free := int64(fs.Bfree) * int64(fs.Bsize)
	used = diskUsageOfSubdirs()
	total = free + used
	pct = int(used * 100 / total)
	return
}

func deleteSomeFiles() {
	// use *find* command to get 5 least-recently-used files under the data/ directory
	script := `find ./data/ -type f -printf "%AY%Am%Ad %AT %p\n" | sort 2> /dev/null | head -5`
	cmd := exec.Command("bash", "-c", script)
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(string(out), "\n")

	// delete those files
	for _, line := range lines {
		for ii, vv := range strings.Fields(line) {
			if ii >= 2 {
				err = os.Remove(vv)
				if err != nil {
					log.Fatal(err)
				}
				break
			}
		}
	}
}

const HWM = 90
const LWM = 80

func Diskmon() {

	for {
		used, total, pct := diskUsage()

		if pct < HWM {
			log.Printf("diskmon: %d out of %d bytes or %d%% -- skip cleanup\n", used, total, pct)
			time.Sleep(30 * time.Second)
			continue
		}

		for pct > LWM {
			log.Printf("diskmon: %d out of %d bytes or %d%% -- commencing cleanup\n", used, total, pct)
			deleteSomeFiles()
			used, total, pct = diskUsage()
		}
	}
}
