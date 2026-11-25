package memo

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// Loader fetches values for keys missing from the cache. Keys absent from the
// returned map are treated as missing. Loader must be safe for concurrent use.
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
	if cfg.StaleWhileRevalidate > 0 && (cfg.Loader == nil || cfg.TTL == 0) {
		errs = append(errs, errors.New("stale-while-revalidate requires a loader and a ttl"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("memo: invalid config: %w", err)
	}
	return nil
}

type entry[V any] struct {
	value     V
	isMissing bool
	expires   time.Time // zero means never
}

type call[V any] struct {
	done    chan struct{}
	value   V
	isFound bool
	err     error
}

// Cache is an in-memory cache. Create it with New. A Cache is safe for
// concurrent use, and concurrent loads of the same key are coalesced into one
// Loader call.
type Cache[K comparable, V any] struct {
	cfg Config[K, V]

	mu       sync.Mutex
	entries  map[K]*entry[V]
	evictor  evictor[K]
	inflight map[K]*call[V]
}

// New validates cfg and returns an empty cache.
func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Cache[K, V]{
		cfg:      cfg,
		entries:  make(map[K]*entry[V], cfg.Capacity),
		evictor:  newEvictor[K](cfg.Policy),
		inflight: make(map[K]*call[V]),
	}, nil
}

// Set stores value under key with the configured TTL.
func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, c.cfg.TTL)
}

// SetWithTTL stores value under key with ttl. Zero means the entry never
// expires; jitter still applies to a positive ttl.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store(key, &entry[V]{value: value, expires: c.expiry(ttl)})
}

// Get returns the value for key. On a miss it calls the Loader, if any, and
// caches the result. found is false when the key is absent or cached as
// missing. err is the Loader's error, or ctx.Err() when ctx ends before the
// load finishes; the load itself keeps running for other callers.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, found bool, err error) {
	c.mu.Lock()
	if e, ok := c.lookup(key); ok {
		c.mu.Unlock()
		return e.value, !e.isMissing, nil
	}
	if c.cfg.Loader == nil {
		c.mu.Unlock()
		var zero V
		return zero, false, nil
	}
	cl := c.startLoads([]K{key})[0]
	c.mu.Unlock()

	select {
	case <-cl.done:
		return cl.value, cl.isFound, cl.err
	case <-ctx.Done():
		var zero V
		return zero, false, ctx.Err()
	}
}

// GetMany returns the values found for keys. Misses are loaded with a single
// Loader call, so a batch-capable Loader, such as one running a SQL IN
// query, fetches them together. On error GetMany returns the values found so
// far together with the error.
func (c *Cache[K, V]) GetMany(ctx context.Context, keys []K) (map[K]V, error) {
	result := make(map[K]V, len(keys))
	var misses []K

	c.mu.Lock()
	for _, key := range keys {
		if _, ok := result[key]; ok || slices.Contains(misses, key) {
			continue
		}
		if e, ok := c.lookup(key); ok {
			if !e.isMissing {
				result[key] = e.value
			}
			continue
		}
		misses = append(misses, key)
	}
	if len(misses) == 0 || c.cfg.Loader == nil {
		c.mu.Unlock()
		return result, nil
	}
	calls := c.startLoads(misses)
	c.mu.Unlock()

	var errs []error
	for i, cl := range calls {
		select {
		case <-cl.done:
		case <-ctx.Done():
			return result, ctx.Err()
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

// lookup returns the usable entry for key and updates its eviction order. An
// expired entry is still usable inside the stale-while-revalidate window,
// and a reload is started for it. lookup must be called with c.mu held.
func (c *Cache[K, V]) lookup(key K) (*entry[V], bool) {
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	now := time.Now()
	switch {
	case e.expires.IsZero() || now.Before(e.expires):
		c.evictor.touch(key)
		return e, true
	case c.cfg.StaleWhileRevalidate > 0 && now.Before(e.expires.Add(c.cfg.StaleWhileRevalidate)):
		c.evictor.touch(key)
		c.startLoads([]K{key})
		return e, true
	default:
		c.delete(key)
		return nil, false
	}
}

// startLoads returns the in-flight load of each key, starting one Loader
// call for the keys that have none. It must be called with c.mu held.
func (c *Cache[K, V]) startLoads(keys []K) []*call[V] {
	calls := make([]*call[V], len(keys))
	var newKeys []K
	var newCalls []*call[V]
	for i, key := range keys {
		if cl, ok := c.inflight[key]; ok {
			calls[i] = cl
			continue
		}
		cl := &call[V]{done: make(chan struct{})}
		c.inflight[key] = cl
		calls[i] = cl
		newKeys = append(newKeys, key)
		newCalls = append(newCalls, cl)
	}
	if len(newKeys) > 0 {
		go c.load(newKeys, newCalls)
	}
	return calls
}

// load runs detached from any caller's context, so that a canceled Get does
// not fail other callers waiting on the same keys.
func (c *Cache[K, V]) load(keys []K, calls []*call[V]) {
	values, err := c.cfg.Loader(context.Background(), keys)
	if err != nil {
		err = fmt.Errorf("memo: load: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i, key := range keys {
		cl := calls[i]
		delete(c.inflight, key)
		if err != nil {
			cl.err = err
			close(cl.done)
			continue
		}
		v, ok := values[key]
		switch {
		case ok:
			c.store(key, &entry[V]{value: v, expires: c.expiry(c.cfg.TTL)})
		case c.cfg.CacheMissing:
			c.store(key, &entry[V]{isMissing: true, expires: c.expiry(c.cfg.TTL)})
		default:
			c.delete(key)
		}
		cl.value, cl.isFound = v, ok
		close(cl.done)
	}
}

// Peek returns the value for key without calling the Loader or updating the
// eviction order. Expired values are not returned.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || e.isMissing || c.isExpired(e, time.Now()) {
		var zero V
		return zero, false
	}
	return e.value, true
}

// Has reports whether key holds a fresh value that is not cached as missing.
func (c *Cache[K, V]) Has(key K) bool {
	_, ok := c.Peek(key)
	return ok
}

// All returns an iterator over a snapshot of the fresh entries, in
// unspecified order. Like Peek, it does not update the eviction order.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	c.mu.Lock()
	now := time.Now()
	snapshot := make(map[K]V, len(c.entries))
	for k, e := range c.entries {
		if !e.isMissing && !c.isExpired(e, now) {
			snapshot[k] = e.value
		}
	}
	c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entries[key]
	c.delete(key)
	return ok
}

// Purge removes every entry.
func (c *Cache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[K]*entry[V], c.cfg.Capacity)
	c.evictor = newEvictor[K](c.cfg.Policy)
}

// Len returns the number of stored entries, including expired entries not yet
// removed and keys cached as missing.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// store must be called with c.mu held.
func (c *Cache[K, V]) store(key K, e *entry[V]) {
	if _, ok := c.entries[key]; !ok {
		for len(c.entries) >= c.cfg.Capacity {
			victim, ok := c.evictor.victim()
			if !ok {
				break
			}
			c.delete(victim)
		}
	}
	c.entries[key] = e
	c.evictor.add(key)
}

// delete must be called with c.mu held.
func (c *Cache[K, V]) delete(key K) {
	delete(c.entries, key)
	c.evictor.remove(key)
}

func (c *Cache[K, V]) isExpired(e *entry[V], now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

func (c *Cache[K, V]) expiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	if c.cfg.Jitter > 0 {
		ttl -= time.Duration(float64(ttl) * c.cfg.Jitter * rand.Float64())
	}
	return time.Now().Add(ttl)
}
