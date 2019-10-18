package main

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// What we used only
func diskUsageOfSubdirs() int64 {
	cmd := exec.Command("du", "-s", ".", "-B", "1048576")
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
		line = strings.Trim(line, " \r\n")
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		fname := parts[2]
		err = os.Remove(fname)
		if err != nil {
			log.Fatal(err)
		}
	}
}

const HWM = 90
const LWM = 80

func diskmon() {

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
