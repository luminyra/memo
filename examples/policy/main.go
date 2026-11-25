// Command policy runs the same access pattern against LRU, LFU and FIFO
// caches and shows which entries each policy keeps.
package main

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/luminyra/memo"
)

func main() {
	ctx := context.Background()

	// region: policy
	for _, policy := range []memo.Policy{memo.LRU, memo.LFU, memo.FIFO} {
		cache, err := memo.New(memo.Config[string, int]{Capacity: 3, Policy: policy})
		if err != nil {
			panic(err)
		}

		// Write a, b and c in order, filling the cache.
		cache.Set("a", 1)
		cache.Set("b", 2)
		cache.Set("c", 3)

		// b is read three times (most frequent), c once, a once (most recent).
		for _, key := range []string{"b", "b", "b", "c", "a"} {
			cache.Get(ctx, key)
		}

		// Writing a fourth key evicts one entry; All iterates over the rest.
		cache.Set("d", 4)
		kept := slices.Sorted(maps.Keys(maps.Collect(cache.All())))
		fmt.Printf("%-4s keeps %v\n", policy, kept)
	}
	// endregion
}
