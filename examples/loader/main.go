// Command loader backs a memo cache with a batch loader: concurrent misses for
// the same key share one load, GetMany loads all its misses in one call, and
// missing keys are cached too.
package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luminyra/memo"
)

func main() {
	ctx := context.Background()

	// region: config
	// The Loader receives a batch of keys, like SELECT ... WHERE id IN (...).
	var loads atomic.Int32
	loadUsers := func(_ context.Context, ids []int) (map[int]string, error) {
		loads.Add(1)
		time.Sleep(50 * time.Millisecond) // a slow query
		users := make(map[int]string)
		for _, id := range ids {
			if id <= 100 { // only users 1 to 100 exist
				users[id] = fmt.Sprintf("user-%d", id)
			}
		}
		return users, nil
	}

	// Only Capacity is required; the zero value of every other field turns
	// its feature off.
	cache, err := memo.New(memo.Config[int, string]{
		Capacity:     1000,
		TTL:          time.Minute,
		Loader:       loadUsers,
		CacheMissing: true, // remember missing keys instead of querying again
	})
	if err != nil {
		panic(err)
	}
	// endregion

	// region: concurrent
	// Ten goroutines read the same uncached key; it is loaded once.
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, _, err := cache.Get(ctx, 1); err != nil {
				panic(err)
			}
		})
	}
	wg.Wait()
	fmt.Println("10 concurrent reads, loads:", loads.Load())
	// endregion

	// region: batch
	// GetMany returns cached keys directly and loads the rest (2, 3, 999) in
	// a single call.
	users, err := cache.GetMany(ctx, []int{1, 2, 3, 999})
	if err != nil {
		panic(err)
	}
	for _, id := range slices.Sorted(maps.Keys(users)) {
		fmt.Println(id, users[id])
	}
	fmt.Println("after GetMany, loads:", loads.Load())
	// endregion

	// region: missing
	// 999 is now cached as missing, so reading it again does not load.
	_, found, _ := cache.Get(ctx, 999)
	fmt.Println("read 999:", found, "loads:", loads.Load())
	// endregion
}
