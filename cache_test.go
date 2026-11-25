package memo

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func mustNew[K comparable, V any](t *testing.T, cfg Config[K, V]) *Cache[K, V] {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestConfigValidation(t *testing.T) {
	loader := func(context.Context, []int) (map[int]int, error) { return nil, nil }
	tests := []struct {
		name string
		cfg  Config[int, int]
	}{
		{name: "capacity", cfg: Config[int, int]{}},
		{name: "policy", cfg: Config[int, int]{Capacity: 1, Policy: Policy(9)}},
		{name: "jitter", cfg: Config[int, int]{Capacity: 1, Jitter: 1}},
		{name: "ttl", cfg: Config[int, int]{Capacity: 1, TTL: -1}},
		{name: "missing without loader", cfg: Config[int, int]{Capacity: 1, CacheMissing: true}},
		{name: "negative stale window", cfg: Config[int, int]{Capacity: 1, StaleWhileRevalidate: -1}},
		{name: "stale without ttl", cfg: Config[int, int]{Capacity: 1, Loader: loader, StaleWhileRevalidate: time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil || !strings.HasPrefix(err.Error(), "memo: invalid config: ") {
				t.Errorf("New accepted an invalid config: %v", err)
			}
		})
	}
}

func TestTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, Config[string, int]{Capacity: 10, TTL: time.Minute})
		c.Set("a", 1)
		c.SetWithTTL("forever", 2, 0)

		time.Sleep(59 * time.Second)
		if !c.Has("a") {
			t.Fatal("entry expired early")
		}
		time.Sleep(time.Second)
		if _, found, _ := c.Get(t.Context(), "a"); found {
			t.Fatal("expired entry returned")
		}
		if c.Len() != 1 || !c.Has("forever") {
			t.Fatal("expired entry not removed or eternal entry lost")
		}
	})
}

func TestJitterShortensTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, Config[int, int]{Capacity: 100, TTL: time.Minute, Jitter: 0.5})
		for i := range 100 {
			c.Set(i, i)
		}
		for _, e := range c.entries {
			ttl := time.Until(e.expires)
			if ttl <= 30*time.Second || ttl > time.Minute {
				t.Fatalf("ttl with jitter = %s", ttl)
			}
		}
	})
}

func TestLoaderCoalescesConcurrentMisses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		loader := func(_ context.Context, keys []string) (map[string]int, error) {
			calls.Add(1)
			<-release
			return map[string]int{keys[0]: len(keys[0])}, nil
		}
		c := mustNew(t, Config[string, int]{Capacity: 10, Loader: loader})

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				if v, found, err := c.Get(t.Context(), "abc"); v != 3 || !found || err != nil {
					t.Errorf("Get = %d, %t, %v", v, found, err)
				}
			})
		}
		synctest.Wait() // every Get is now waiting on the same load
		close(release)
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("loader calls = %d, want 1", calls.Load())
		}
		if !c.Has("abc") {
			t.Fatal("loaded value not cached")
		}
	})
}

func TestGetManyBatchesMisses(t *testing.T) {
	var batches [][]int
	var mu sync.Mutex
	loader := func(_ context.Context, ids []int) (map[int]string, error) {
		mu.Lock()
		batches = append(batches, slices.Clone(ids))
		mu.Unlock()
		users := make(map[int]string)
		for _, id := range ids {
			if id < 100 {
				users[id] = "user"
			}
		}
		return users, nil
	}
	c := mustNew(t, Config[int, string]{Capacity: 10, Loader: loader, CacheMissing: true})
	c.Set(1, "cached")

	got, err := c.GetMany(t.Context(), []int{1, 2, 3, 2, 404})
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, map[int]string{1: "cached", 2: "user", 3: "user"}) {
		t.Errorf("GetMany = %v", got)
	}
	if len(batches) != 1 || !slices.Equal(batches[0], []int{2, 3, 404}) {
		t.Errorf("loader batches = %v, want one batch [2 3 404]", batches)
	}

	// Everything is cached now, including the missing key 404.
	if got, _ := c.GetMany(t.Context(), []int{2, 404}); !maps.Equal(got, map[int]string{2: "user"}) || len(batches) != 1 {
		t.Errorf("second GetMany = %v after %d batches", got, len(batches))
	}

	noLoader := mustNew(t, Config[int, string]{Capacity: 10})
	noLoader.Set(1, "a")
	if got, err := noLoader.GetMany(t.Context(), []int{1, 2}); err != nil || !maps.Equal(got, map[int]string{1: "a"}) {
		t.Errorf("GetMany without loader = %v, %v", got, err)
	}
}

func TestGetManyErrors(t *testing.T) {
	boom := errors.New("boom")
	failing := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
		return nil, boom
	}})
	failing.Set(1, 1)
	got, err := failing.GetMany(t.Context(), []int{1, 2, 3})
	if !errors.Is(err, boom) || strings.Count(err.Error(), "boom") != 1 || !maps.Equal(got, map[int]int{1: 1}) {
		t.Errorf("GetMany = %v, %v", got, err)
	}

	block := make(chan struct{})
	defer close(block)
	slow := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
		<-block
		return nil, nil
	}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := slow.GetMany(ctx, []int{1}); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled GetMany error = %v", err)
	}
}

func TestLoaderErrorAndCancel(t *testing.T) {
	boom := errors.New("boom")
	c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
		return nil, boom
	}})
	if _, _, err := c.Get(t.Context(), 1); !errors.Is(err, boom) {
		t.Fatalf("Get error = %v", err)
	}

	block := make(chan struct{})
	defer close(block)
	slow := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
		<-block
		return nil, nil
	}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := slow.Get(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get error = %v", err)
	}
}

func TestGetWithoutLoader(t *testing.T) {
	c := mustNew(t, Config[int, int]{Capacity: 1})
	if _, found, err := c.Get(t.Context(), 1); found || err != nil {
		t.Fatalf("Get = %t, %v", found, err)
	}
}

func TestCacheMissing(t *testing.T) {
	var calls atomic.Int32
	loader := func(context.Context, []int) (map[int]int, error) {
		calls.Add(1)
		return map[int]int{}, nil
	}
	c := mustNew(t, Config[int, int]{Capacity: 10, Loader: loader, CacheMissing: true})
	for range 3 {
		if _, found, err := c.Get(t.Context(), 7); found || err != nil {
			t.Fatalf("Get = %t, %v", found, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}
	if c.Has(7) {
		t.Fatal("missing key reported as present")
	}

	uncached := mustNew(t, Config[int, int]{Capacity: 10, Loader: loader})
	uncached.Get(t.Context(), 7)
	if uncached.Len() != 0 {
		t.Fatal("a missing key was cached without CacheMissing")
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var version atomic.Int32
		loader := func(context.Context, []string) (map[string]int32, error) {
			return map[string]int32{"k": version.Add(1)}, nil
		}
		c := mustNew(t, Config[string, int32]{
			Capacity:             10,
			TTL:                  time.Minute,
			Loader:               loader,
			StaleWhileRevalidate: time.Minute,
		})

		if v, _, _ := c.Get(t.Context(), "k"); v != 1 {
			t.Fatalf("first Get = %d", v)
		}
		time.Sleep(90 * time.Second) // expired, inside the revalidation window
		if v, found, _ := c.Get(t.Context(), "k"); v != 1 || !found {
			t.Fatalf("stale Get = %d, %t", v, found)
		}
		synctest.Wait() // the background reload has finished
		if v, ok := c.Peek("k"); !ok || v != 2 {
			t.Fatalf("value after revalidation = %d, %t", v, ok)
		}

		time.Sleep(3 * time.Minute) // past the revalidation window
		if v, _, _ := c.Get(t.Context(), "k"); v != 3 {
			t.Fatalf("Get after the window must load synchronously, got %d", v)
		}
	})
}

func TestAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		loader := func(context.Context, []int) (map[int]int, error) { return nil, nil }
		c := mustNew(t, Config[int, int]{Capacity: 10, TTL: time.Minute, Loader: loader, CacheMissing: true})
		c.Set(1, 10)
		c.SetWithTTL(2, 20, time.Second)
		c.Get(t.Context(), 3) // cached as missing

		time.Sleep(2 * time.Second)
		if got := maps.Collect(c.All()); !maps.Equal(got, map[int]int{1: 10}) {
			t.Errorf("All = %v", got)
		}
		for range c.All() {
			break // stopping early must be safe
		}
	})
}

func TestDeleteAndPurge(t *testing.T) {
	c := mustNew(t, Config[int, int]{Capacity: 10, Policy: LFU})
	c.Set(1, 1)
	c.Set(2, 2)
	if !c.Delete(1) || c.Delete(1) || c.Has(1) {
		t.Fatal("Delete mismatch")
	}
	c.Purge()
	if c.Len() != 0 || c.Has(2) {
		t.Fatal("Purge left entries")
	}
	c.Set(3, 3) // evictor must be usable after Purge
	if !c.Has(3) {
		t.Fatal("Set after Purge failed")
	}
}
