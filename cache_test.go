package memo

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/luminyra/conc"
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
		{name: "negative max loads", cfg: Config[int, int]{Capacity: 1, Loader: loader, MaxConcurrentLoads: -1}},
		{name: "max loads without loader", cfg: Config[int, int]{Capacity: 1, MaxConcurrentLoads: 1}},
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
		sh := &c.shards[0]
		for _, i := range sh.store.index {
			ttl := time.Duration(sh.store.nodes[i].expires - c.now())
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

func BenchmarkGetHit(b *testing.B) {
	c, _ := New(Config[int, int]{Capacity: 1024})
	for i := range 1024 {
		c.Set(i, i)
	}
	ctx := b.Context()
	i := 0
	for b.Loop() {
		c.Get(ctx, i&1023)
		i++
	}
}

func BenchmarkGetHitParallel(b *testing.B) {
	c, _ := New(Config[int, int]{Capacity: 1024})
	for i := range 1024 {
		c.Set(i, i)
	}
	ctx := b.Context()
	var seed atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine reads its own random sequence of keys, as
		// independent requests would.
		rng := rand.New(rand.NewPCG(seed.Add(1), 0))
		for pb.Next() {
			c.Get(ctx, rng.IntN(1024))
		}
	})
}

func BenchmarkSetWithEviction(b *testing.B) {
	for _, policy := range []Policy{LRU, LFU, FIFO} {
		b.Run(policy.String(), func(b *testing.B) {
			c, _ := New(Config[int, int]{Capacity: 1024, Policy: policy})
			i := 0
			for b.Loop() {
				c.Set(i, i) // every new key past 1024 evicts one entry
				i++
			}
		})
	}
}

func BenchmarkGetManyHit(b *testing.B) {
	c, _ := New(Config[int, int]{Capacity: 1024, Loader: func(context.Context, []int) (map[int]int, error) { return nil, nil }})
	keys := make([]int, 64)
	for i := range keys {
		keys[i] = i
		c.Set(i, i)
	}
	ctx := b.Context()
	for b.Loop() {
		c.GetMany(ctx, keys)
	}
}

func TestShards(t *testing.T) {
	if _, err := New(Config[int, int]{Capacity: 1, Shards: -1}); err == nil {
		t.Fatal("negative Shards accepted")
	}

	var batches [][]int
	var mu sync.Mutex
	loader := func(_ context.Context, ids []int) (map[int]int, error) {
		mu.Lock()
		batches = append(batches, slices.Clone(ids))
		mu.Unlock()
		values := make(map[int]int, len(ids))
		for _, id := range ids {
			values[id] = id * 10
		}
		return values, nil
	}
	c := mustNew(t, Config[int, int]{Capacity: 64, Shards: 4, Loader: loader})
	for i := range 32 {
		c.Set(i, i)
	}
	if c.Len() != 32 {
		t.Fatalf("Len = %d", c.Len())
	}
	for i := range 32 {
		if v, ok := c.Peek(i); !ok || v != i {
			t.Fatalf("Peek(%d) = %d, %t", i, v, ok)
		}
	}
	// Misses spread over every shard still load in one call.
	got, err := c.GetMany(t.Context(), []int{100, 101, 102, 103, 104, 105, 106, 107})
	if err != nil || len(got) != 8 || got[105] != 1050 || len(batches) != 1 {
		t.Fatalf("GetMany = %v, %v after %d batches", got, err, len(batches))
	}
	if !c.Delete(3) || c.Has(3) {
		t.Fatal("Delete across shards failed")
	}
	if n := len(maps.Collect(c.All())); n != 39 {
		t.Fatalf("All has %d entries, want 39", n)
	}
	c.Purge()
	if c.Len() != 0 {
		t.Fatalf("Len after Purge = %d", c.Len())
	}
}

func TestShardCapacityIsBounded(t *testing.T) {
	c := mustNew(t, Config[int, int]{Capacity: 10, Shards: 4}) // 3 entries per shard
	for i := range 1000 {
		c.Set(i, i)
	}
	if n := c.Len(); n > 12 {
		t.Fatalf("Len = %d, want at most 4 shards × 3 entries", n)
	}
}

func BenchmarkGetHitParallelSharded(b *testing.B) {
	c, _ := New(Config[int, int]{Capacity: 1024, Shards: 16})
	for i := range 1024 {
		c.Set(i, i)
	}
	ctx := b.Context()
	var seed atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine reads its own random sequence of keys, as
		// independent requests would.
		rng := rand.New(rand.NewPCG(seed.Add(1), 0))
		for pb.Next() {
			c.Get(ctx, rng.IntN(1024))
		}
	})
}

func TestLoaderAbnormalExit(t *testing.T) {
	t.Run("goexit", func(t *testing.T) {
		c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
			runtime.Goexit()
			return nil, nil
		}})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := c.Get(ctx, 1); !errors.Is(err, conc.ErrGoexit) {
			t.Fatalf("Get error = %v, want ErrGoexit", err)
		}
		got, err := c.GetMany(ctx, []int{2, 3})
		if !errors.Is(err, conc.ErrGoexit) || len(got) != 0 {
			t.Fatalf("GetMany = %v, %v, want ErrGoexit", got, err)
		}
	})
	t.Run("panic", func(t *testing.T) {
		var calls atomic.Int32
		c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(_ context.Context, keys []int) (map[int]int, error) {
			if calls.Add(1) == 1 {
				panic("loader bug")
			}
			return map[int]int{keys[0]: 10}, nil
		}})
		p, ok := catchPanic(func() { _, _, _ = c.Get(t.Context(), 1) }).(conc.Panic)
		if !ok || p.Value != "loader bug" || !strings.Contains(p.Stack, "TestLoaderAbnormalExit") {
			t.Fatalf("Get panic = %#v, want conc.Panic from the loader", p)
		}
		// The failed load left nothing behind, so the next Get loads again.
		if v, found, err := c.Get(t.Context(), 1); v != 10 || !found || err != nil {
			t.Fatalf("Get after panic = %d, %t, %v", v, found, err)
		}
	})
	t.Run("panic in GetMany", func(t *testing.T) {
		c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(context.Context, []int) (map[int]int, error) {
			panic("batch bug")
		}})
		if p, ok := catchPanic(func() { _, _ = c.GetMany(t.Context(), []int{1, 2}) }).(conc.Panic); !ok || p.Value != "batch bug" {
			t.Fatalf("GetMany panic = %#v", p)
		}
	})
}

func catchPanic(fn func()) (v any) {
	defer func() { v = recover() }()
	fn()
	return nil
}

func TestEndedContextStartsNoLoad(t *testing.T) {
	var calls atomic.Int32
	c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(_ context.Context, keys []int) (map[int]int, error) {
		calls.Add(1)
		return nil, nil
	}})
	c.Set(1, 1)
	cause := errors.New("request gone")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)

	if v, found, err := c.Get(ctx, 1); v != 1 || !found || err != nil {
		t.Fatalf("hit with ended ctx = %d, %t, %v", v, found, err)
	}
	if _, _, err := c.Get(ctx, 2); !errors.Is(err, cause) {
		t.Fatalf("miss with ended ctx error = %v, want cause", err)
	}
	got, err := c.GetMany(ctx, []int{1, 2})
	if !errors.Is(err, cause) || !maps.Equal(got, map[int]int{1: 1}) {
		t.Fatalf("GetMany with ended ctx = %v, %v", got, err)
	}
	if got, err := c.GetMany(ctx, []int{1}); err != nil || got[1] != 1 {
		t.Fatalf("GetMany hits with ended ctx = %v, %v", got, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", calls.Load())
	}
}

func TestCanceledWaitersLeave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		c := mustNew(t, Config[int, int]{Capacity: 10, Loader: func(_ context.Context, keys []int) (map[int]int, error) {
			<-release
			return nil, nil
		}})
		ctx, cancel := context.WithCancel(t.Context())
		errs := make(chan error, 2)
		go func() { _, _, err := c.Get(ctx, 1); errs <- err }()
		go func() { _, err := c.GetMany(ctx, []int{2, 1, 3}); errs <- err }()
		synctest.Wait()
		cancel()
		for range 2 {
			if err := <-errs; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled wait error = %v", err)
			}
		}
		for _, key := range []int{1, 2, 3} {
			sh := c.shardFor(key)
			sh.mu.Lock()
			if n := sh.inflight[key].waiters; n != 0 {
				t.Errorf("key %d has %d waiters after cancellation", key, n)
			}
			sh.mu.Unlock()
		}
		close(release)
		synctest.Wait()
		if c.Len() != 0 {
			t.Errorf("Len = %d after loads of missing keys", c.Len())
		}
	})
}

func TestMaxConcurrentLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running, peak atomic.Int32
		release := make(chan struct{})
		c := mustNew(t, Config[int, int]{Capacity: 100, MaxConcurrentLoads: 2, Loader: func(_ context.Context, keys []int) (map[int]int, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			running.Add(-1)
			return map[int]int{keys[0]: keys[0]}, nil
		}})
		var wg sync.WaitGroup
		for key := range 5 {
			wg.Go(func() {
				if v, found, err := c.Get(t.Context(), key); v != key || !found || err != nil {
					t.Errorf("Get(%d) = %d, %t, %v", key, v, found, err)
				}
			})
		}
		synctest.Wait()
		if n := running.Load(); n != 2 {
			t.Fatalf("running loads = %d, want 2", n)
		}
		close(release)
		wg.Wait()
		if p := peak.Load(); p != 2 {
			t.Fatalf("peak concurrent loads = %d, want 2", p)
		}
	})
}
