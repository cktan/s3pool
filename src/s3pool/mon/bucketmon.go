package mon

import (
	"log"
	"s3pool/op"
	"time"
)

func Bucketmon() chan<- string {
	const REFRESHINTERVAL = 15 // minutes
	ch := make(chan string, 10)

	go func() {
		tick := time.Tick(REFRESHINTERVAL * time.Minute)
		bktmap := make(map[string](bool))
		for {
			select {
			case bkt := <-ch:
				bktmap[bkt] = true
			case <-tick:
				for bkt := range bktmap {
					log.Println("bucketmon refresh", bkt)
					_, err := op.Refresh([]string{bkt})
					if err != nil {
						log.Printf("WARNING: autorefresh %s failed: %v\n", bkt, err)
					}
				}
			}
		}
	}()

	return ch
}
