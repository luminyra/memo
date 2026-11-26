// Package memo is an in-memory cache for Go 1.27 with pluggable eviction
// policies, TTL with jitter, batch loaders, missing-key caching and
// stale-while-revalidate.
//
// # Configuration
//
// A [Cache] is configured with a plain [Config] struct, where only Capacity
// is required, and created with [New]:
//
//	users, err := memo.New(memo.Config[int, User]{
//		Capacity: 10_000,
//		TTL:      time.Minute,
//		Loader:   loadUsers, // func(ctx context.Context, ids []int) (map[int]User, error)
//	})
//
// # Loading
//
// A [Loader] takes a batch of keys. [Cache.Get] loads a single key, while
// [Cache.GetMany] loads all of its misses with one Loader call, so a
// database-backed Loader can run a single IN query. Concurrent loads of the
// same key are coalesced into one call. With Config.CacheMissing, keys the
// Loader does not return are remembered until their TTL expires; with
// Config.StaleWhileRevalidate, an expired value is served while it is
// reloaded in the background. Config.MaxConcurrentLoads caps the number of
// Loader calls running at once. Every load finishes its waiters however the
// Loader ends: a panic reaches the waiting callers as a [conc.Panic].
//
// # Behavior
//
// A Cache is safe for concurrent use. Config.Shards splits it into
// independently locked parts for heavily concurrent workloads, and writes
// reuse memory instead of allocating. [Cache.All] iterates over a snapshot of
// the fresh entries. Expired entries are removed lazily on access or when
// evicted, so [Cache.Len] may count entries that have expired but have not
// been removed yet. Time-dependent behavior uses the [time] package directly,
// so tests can control it with [testing/synctest].
//
// The package is inspired by samber/hot and the HTTP stale-while-revalidate
// cache directive.
package memo
