// Command shards splits a memo cache into independently locked shards so that
// concurrent writers working on different keys rarely wait for each other.
package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/luminyra/memo"
)

func main() {
	// region: config
	// Each shard has its own lock and holds Capacity/Shards entries (rounded
	// up). Keys are assigned to shards by hash.
	cache, err := memo.New(memo.Config[int, string]{
		Capacity: 100_000,
		Shards:   runtime.GOMAXPROCS(0),
	})
	if err != nil {
		panic(err)
	}
	// endregion

	// region: concurrent
	// Eight goroutines write different keys; most writes lock different shards.
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				cache.Set(worker*1000+i, fmt.Sprintf("user-%d", worker*1000+i))
			}
		})
	}
	wg.Wait()
	fmt.Println("entries:", cache.Len())
	// endregion
}
