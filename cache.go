package memo

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/luminyra/conc"
)

// Loader fetches values for keys missing from the cache. Keys absent from the
// returned map are treated as missing. Loader must be safe for concurrent use.
// It runs in its own goroutine, detached from the callers that wait for it.
type Loader[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// Config configures a Cache. Only Capacity is required; the zero value of
// every other field disables the feature it controls.
type Config[K comparable, V any] struct {
	// Capacity is the maximum number of entries. It must be positive.
	Capacity int

	// Policy selects the entry to evict when the cache is full. The zero
	// value is LRU.
	Policy Policy

	// TTL is how long entries stay fresh. Zero means entries never expire.
	TTL time.Duration

	// Jitter shortens each TTL by a random fraction in [0, Jitter), so that
	// entries written together do not expire together. It must be in [0, 1).
	Jitter float64

	// Loader is called by Get and GetMany on a miss.
	Loader Loader[K, V]

	// CacheMissing makes the cache remember keys that Loader did not return,
	// so they are not loaded again until their TTL expires. It requires a
	// Loader.
	CacheMissing bool

	// StaleWhileRevalidate lets Get serve an expired value for up to this
	// long after its TTL while it is reloaded in the background. It requires
	// a Loader and a TTL.
	StaleWhileRevalidate time.Duration

	// MaxConcurrentLoads limits how many Loader calls run at once, so a burst
	// of misses cannot overload the data source. Further loads queue in
	// arrival order; their callers keep waiting and may give up through their
	// own contexts. Zero means no limit.
	MaxConcurrentLoads int

	// Shards splits the cache into this many independently locked parts,
	// chosen by a hash of the key, so that concurrent callers working on
	// different keys rarely wait for each other. Each shard holds
	// Capacity/Shards entries (rounded up) and applies Policy on its own, so
	// eviction order is exact within a shard only. Zero or one means a single
	// shard. Use about as many shards as busy goroutines, such as
	// runtime.GOMAXPROCS(0), when profiling shows contention.
	Shards int
}

func (cfg Config[K, V]) validate() error {
	var errs []error
	if !cfg.Policy.isValid() {
		errs = append(errs, fmt.Errorf("unknown policy %d", int(cfg.Policy)))
	}
	if cfg.Capacity <= 0 {
		errs = append(errs, fmt.Errorf("capacity must be positive, got %d", cfg.Capacity))
	}
	if cfg.TTL < 0 {
		errs = append(errs, fmt.Errorf("ttl must not be negative, got %s", cfg.TTL))
	}
	if cfg.Jitter < 0 || cfg.Jitter >= 1 {
		errs = append(errs, fmt.Errorf("jitter must be in [0, 1), got %v", cfg.Jitter))
	}
	if cfg.CacheMissing && cfg.Loader == nil {
		errs = append(errs, errors.New("caching missing keys requires a loader"))
	}
	if cfg.StaleWhileRevalidate < 0 {
		errs = append(errs, fmt.Errorf("stale-while-revalidate must not be negative, got %s", cfg.StaleWhileRevalidate))
	}
	if cfg.MaxConcurrentLoads < 0 {
		errs = append(errs, fmt.Errorf("max concurrent loads must not be negative, got %d", cfg.MaxConcurrentLoads))
	}
	if cfg.MaxConcurrentLoads > 0 && cfg.Loader == nil {
		errs = append(errs, errors.New("max concurrent loads requires a loader"))
	}
	if cfg.Shards < 0 {
		errs = append(errs, fmt.Errorf("shards must not be negative, got %d", cfg.Shards))
	}
	if cfg.StaleWhileRevalidate > 0 && (cfg.Loader == nil || cfg.TTL == 0) {
		errs = append(errs, errors.New("stale-while-revalidate requires a loader and a ttl"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("memo: invalid config: %w", err)
	}
	return nil
}

// call is the in-flight load of one key. Its result fields are written once,
// before done is closed.
type call[V any] struct {
	done    chan struct{}
	waiters int // callers still waiting, protected by the lock of the key's shard
	value   V
	isFound bool
	err     error
	failure *conc.Panic
}

// Cache is an in-memory cache. Create it with New. A Cache is safe for
// concurrent use, and concurrent loads of the same key are coalesced into one
// Loader call.
type Cache[K comparable, V any] struct {
	cfg    Config[K, V]
	epoch  time.Time // expiration times are monotonic offsets from epoch
	seed   maphash.Seed
	shards []shard[K, V]
	loads  *conc.Semaphore // nil without Config.MaxConcurrentLoads
}

type shard[K comparable, V any] struct {
	mu       sync.Mutex
	store    store[K, V]
	inflight map[K]*call[V]
	_        [64]byte // keep the locks of neighboring shards on separate cache lines
}

// New validates cfg and returns an empty cache.
func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	n := max(cfg.Shards, 1)
	perShard := (cfg.Capacity + n - 1) / n
	c := &Cache[K, V]{cfg: cfg, epoch: time.Now(), seed: maphash.MakeSeed(), shards: make([]shard[K, V], n)}
	for i := range c.shards {
		c.shards[i].store = newStore[K, V](cfg.Policy, perShard)
		c.shards[i].inflight = make(map[K]*call[V])
	}
	if cfg.MaxConcurrentLoads > 0 {
		c.loads = conc.NewSemaphore(int64(cfg.MaxConcurrentLoads))
	}
	return c, nil
}

func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	return &c.shards[maphash.Comparable(c.seed, key)%uint64(len(c.shards))]
}

// now returns the monotonic time since the cache was created. Unlike wall
// clock times, it is unaffected by clock adjustments and fits in 8 bytes.
func (c *Cache[K, V]) now() int64 {
	return int64(time.Since(c.epoch))
}

// Set stores value under key with the configured TTL.
func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, c.cfg.TTL)
}

// SetWithTTL stores value under key with ttl. Zero means the entry never
// expires; jitter still applies to a positive ttl.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	expires := c.expiry(ttl)
	sh := c.shardFor(key)
	sh.mu.Lock()
	sh.store.put(key, value, expires, false)
	sh.mu.Unlock()
}

// Get returns the value for key. On a miss it calls the Loader, if any, and
// caches the result. found is false when the key is absent or cached as
// missing. err is the Loader's error, or [context.Cause] of ctx when ctx ends
// before the load finishes; the load itself keeps running for other callers.
// A cached value is returned even if ctx has ended, but a miss with an ended
// ctx returns its cause without starting a load.
//
// If the Loader panics, Get re-panics with a [conc.Panic] holding the
// original value and stack. If no caller is waiting for the load, the
// loading goroutine re-panics instead. A Loader that calls [runtime.Goexit]
// fails the load with an error matching [conc.ErrGoexit].
func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, found bool, err error) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	if v, isMissing, ok := c.lookup(sh, key); ok {
		sh.mu.Unlock()
		return v, !isMissing, nil
	}
	if c.cfg.Loader == nil {
		sh.mu.Unlock()
		return value, false, nil
	}
	if ctx.Err() != nil {
		sh.mu.Unlock()
		return value, false, context.Cause(ctx)
	}
	cl, isNew := startLoad(sh, key)
	cl.waiters++
	sh.mu.Unlock()
	if isNew {
		go c.load([]K{key}, []*call[V]{cl})
	}

	if err := await(ctx, sh, cl); err != nil {
		return value, false, err
	}
	if cl.failure != nil {
		panic(*cl.failure)
	}
	return cl.value, cl.isFound, cl.err
}

// GetMany returns the values found for keys. Misses are loaded with a single
// Loader call, so a batch-capable Loader, such as one running a SQL IN
// query, fetches them together. On error GetMany returns the values found so
// far together with the error. Like [Cache.Get], it returns cached values
// even if ctx has ended but starts no load then, and it re-panics with a
// [conc.Panic] if a Loader it waits for panics.
func (c *Cache[K, V]) GetMany(ctx context.Context, keys []K) (map[K]V, error) {
	ended := ctx.Err() != nil // checked once, so a load is never claimed without being started
	result := make(map[K]V, len(keys))
	var (
		misses   []K // keys whose load this call waits for
		calls    []*call[V]
		newKeys  []K // keys whose load this call starts
		newCalls []*call[V]
		skipped  bool // a miss was not loaded because ctx had ended
	)
	for _, key := range keys {
		if _, ok := result[key]; ok || slices.Contains(misses, key) {
			continue
		}
		sh := c.shardFor(key)
		sh.mu.Lock()
		if v, isMissing, ok := c.lookup(sh, key); ok {
			sh.mu.Unlock()
			if !isMissing {
				result[key] = v
			}
			continue
		}
		if c.cfg.Loader == nil || ended {
			sh.mu.Unlock()
			skipped = skipped || c.cfg.Loader != nil
			continue
		}
		cl, isNew := startLoad(sh, key)
		cl.waiters++
		sh.mu.Unlock()
		misses, calls = append(misses, key), append(calls, cl)
		if isNew {
			newKeys, newCalls = append(newKeys, key), append(newCalls, cl)
		}
	}
	if len(newKeys) > 0 {
		go c.load(newKeys, newCalls)
	}
	if skipped {
		return result, context.Cause(ctx)
	}

	var errs []error
	for i, cl := range calls {
		err := await(ctx, c.shardFor(misses[i]), cl)
		if err != nil || cl.failure != nil {
			for j := i + 1; j < len(calls); j++ {
				leave(c.shardFor(misses[j]), calls[j])
			}
			if err != nil {
				return result, err
			}
			panic(*cl.failure)
		}
		switch {
		case cl.err != nil:
			if !slices.Contains(errs, cl.err) {
				errs = append(errs, cl.err)
			}
		case cl.isFound:
			result[misses[i]] = cl.value
		}
	}
	return result, errors.Join(errs...)
}

// lookup returns the usable value for key and records the use. An expired
// entry is still usable inside the stale-while-revalidate window, and a
// reload is started for it. lookup must be called with sh.mu held.
func (c *Cache[K, V]) lookup(sh *shard[K, V], key K) (value V, isMissing, ok bool) {
	i, ok := sh.store.lookup(key)
	if !ok {
		return value, false, false
	}
	n := &sh.store.nodes[i]
	value, isMissing = n.value, n.isMissing
	if n.expires == 0 { // no TTL: skip reading the clock
		sh.store.touch(i)
		return value, isMissing, true
	}
	now := c.now()
	switch {
	case now < n.expires:
		sh.store.touch(i)
		return value, isMissing, true
	case c.cfg.StaleWhileRevalidate > 0 && now < n.expires+int64(c.cfg.StaleWhileRevalidate):
		sh.store.touch(i)
		if cl, isNew := startLoad(sh, key); isNew {
			go c.load([]K{key}, []*call[V]{cl})
		}
		return value, isMissing, true
	default:
		sh.store.removeAt(i)
		var zero V
		return zero, false, false
	}
}

// startLoad returns the in-flight load of key and reports whether the caller
// must start it. It must be called with sh.mu held.
func startLoad[K comparable, V any](sh *shard[K, V], key K) (*call[V], bool) {
	if cl, ok := sh.inflight[key]; ok {
		return cl, false
	}
	cl := &call[V]{done: make(chan struct{})}
	sh.inflight[key] = cl
	return cl, true
}

// await waits for cl to finish and returns nil, or returns the cause of ctx
// after removing the caller from the waiters of cl. The caller must have
// registered itself as a waiter of cl under the lock of sh.
func await[K comparable, V any](ctx context.Context, sh *shard[K, V], cl *call[V]) error {
	select {
	case <-cl.done:
		return nil
	case <-ctx.Done():
		if leave(sh, cl) {
			return context.Cause(ctx)
		}
		return nil // the load finished first
	}
}

// leave removes a waiter from cl unless the load has already finished, and
// reports whether it did.
func leave[K comparable, V any](sh *shard[K, V], cl *call[V]) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	select {
	case <-cl.done:
		return false
	default:
		cl.waiters--
		return true
	}
}

// load runs detached from any caller's context, so that a canceled Get does
// not fail other callers waiting on the same keys. Every call is finished
// however the Loader ends, so no caller waits forever.
func (c *Cache[K, V]) load(keys []K, calls []*call[V]) {
	if c.loads != nil {
		_ = c.loads.Acquire(context.Background(), 1) // cannot fail without cancellation
		defer c.loads.Release(1)
	}
	var values map[K]V
	conc.Run(func() error {
		var err error
		values, err = c.cfg.Loader(context.Background(), keys)
		return err
	}, func(err error, p *conc.Panic) {
		c.finishLoad(keys, calls, values, err, p)
	})
}

// finishLoad writes the outcome of a Loader call back to the cache, publishes
// it to the waiters and removes the calls from the in-flight maps. A panic
// that no waiter will observe is re-panicked here, as an unrecovered panic in
// the Loader would be.
func (c *Cache[K, V]) finishLoad(keys []K, calls []*call[V], values map[K]V, err error, p *conc.Panic) {
	if err != nil {
		err = fmt.Errorf("memo: load: %w", err)
	}
	isObserved := false
	for i, key := range keys {
		cl := calls[i]
		sh := c.shardFor(key)
		sh.mu.Lock()
		delete(sh.inflight, key)
		switch {
		case p != nil:
			cl.failure = p
		case err != nil:
			cl.err = err
		default:
			v, ok := values[key]
			switch {
			case ok:
				sh.store.put(key, v, c.expiry(c.cfg.TTL), false)
			case c.cfg.CacheMissing:
				var zero V
				sh.store.put(key, zero, c.expiry(c.cfg.TTL), true)
			default:
				sh.store.remove(key)
			}
			cl.value, cl.isFound = v, ok
		}
		isObserved = isObserved || cl.waiters > 0
		close(cl.done)
		sh.mu.Unlock()
	}
	if p != nil && !isObserved {
		panic(*p)
	}
}

// Peek returns the value for key without calling the Loader or updating the
// eviction order. Expired values are not returned.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if i, ok := sh.store.lookup(key); ok {
		if n := &sh.store.nodes[i]; !n.isMissing && !c.isExpired(n) {
			return n.value, true
		}
	}
	var zero V
	return zero, false
}

// Has reports whether key holds a fresh value that is not cached as missing.
func (c *Cache[K, V]) Has(key K) bool {
	_, ok := c.Peek(key)
	return ok
}

// All returns an iterator over a snapshot of the fresh entries, in
// unspecified order. Like Peek, it does not update the eviction order.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	snapshot := make(map[K]V)
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for _, j := range sh.store.index {
			if n := &sh.store.nodes[j]; !n.isMissing && !c.isExpired(n) {
				snapshot[n.key] = n.value
			}
		}
		sh.mu.Unlock()
	}
	return func(yield func(K, V) bool) {
		for k, v := range snapshot {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Delete removes key and reports whether it was present.
func (c *Cache[K, V]) Delete(key K) bool {
	sh := c.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.store.remove(key)
}

// Purge removes every entry.
func (c *Cache[K, V]) Purge() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.store.reset()
		sh.mu.Unlock()
	}
}

// Len returns the number of stored entries, including expired entries not yet
// removed and keys cached as missing.
func (c *Cache[K, V]) Len() int {
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n += sh.store.len()
		sh.mu.Unlock()
	}
	return n
}

func (c *Cache[K, V]) isExpired(n *node[K, V]) bool {
	return n.expires != 0 && c.now() >= n.expires
}

// expiry returns the expiration time for an entry written now with ttl, or 0
// when it never expires.
func (c *Cache[K, V]) expiry(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	if c.cfg.Jitter > 0 {
		ttl -= time.Duration(float64(ttl) * c.cfg.Jitter * rand.Float64())
	}
	return c.now() + int64(ttl)
}
