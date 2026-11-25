package memo_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/luminyra/memo"
)

func Example() {
	loadUsers := func(_ context.Context, ids []int) (map[int]string, error) {
		fmt.Println("loading", ids)
		users := make(map[int]string)
		for _, id := range ids {
			users[id] = fmt.Sprintf("user-%d", id)
		}
		return users, nil
	}

	cache, err := memo.New(memo.Config[int, string]{Capacity: 100, TTL: time.Minute, Loader: loadUsers})
	if err != nil {
		panic(err)
	}

	name, found, _ := cache.Get(context.Background(), 1)
	fmt.Println(name, found)
	name, found, _ = cache.Get(context.Background(), 1) // served from the cache
	fmt.Println(name, found)
	// Output:
	// loading [1]
	// user-1 true
	// user-1 true
}

func ExampleCache_GetMany() {
	loadUsers := func(_ context.Context, ids []int) (map[int]string, error) {
		fmt.Println("one loader call for", ids)
		return map[int]string{2: "bob", 3: "carol"}, nil
	}
	cache, _ := memo.New(memo.Config[int, string]{Capacity: 100, Loader: loadUsers})
	cache.Set(1, "alice")

	users, _ := cache.GetMany(context.Background(), []int{1, 2, 3, 404})
	for _, id := range slices.Sorted(maps.Keys(users)) {
		fmt.Println(id, users[id])
	}
	// Output:
	// one loader call for [2 3 404]
	// 1 alice
	// 2 bob
	// 3 carol
}

func ExamplePolicy() {
	for _, policy := range []memo.Policy{memo.LRU, memo.LFU, memo.FIFO} {
		cache, _ := memo.New(memo.Config[string, int]{Capacity: 2, Policy: policy})
		cache.Set("a", 1)
		cache.Set("b", 2)
		cache.Get(context.Background(), "a") // a is now recently and frequently used
		cache.Set("c", 3)                    // evicts one entry

		kept := slices.Sorted(maps.Keys(maps.Collect(cache.All())))
		fmt.Println(policy, kept)
	}
	// Output:
	// LRU [a c]
	// LFU [a c]
	// FIFO [b c]
}
