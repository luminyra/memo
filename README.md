<img align="right" src="https://images.luminyra.dev/luminyra/github/memo/logo.webp" alt="A golden key resting in one olive cache slot." width="96" height="96">

# memo

[![Go Reference](https://pkg.go.dev/badge/github.com/luminyra/memo.svg)](https://pkg.go.dev/github.com/luminyra/memo)
[![CI](https://github.com/luminyra/memo/actions/workflows/ci.yml/badge.svg)](https://github.com/luminyra/memo/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/luminyra/memo)](https://goreportcard.com/report/github.com/luminyra/memo)
[![Go 1.27+](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/devel/release)

memo keeps the results of slow lookups in memory, so your program asks the database, an API or the disk once instead of on every request.

You give it a function that loads values, and memo takes care of the rest: it remembers results, expires them after a while, evicts old entries when it is full, and makes sure that a hundred concurrent requests for the same key trigger only one load.

- **Read-through loading.** `Get` calls your loader on a miss; `GetMany` loads all missing keys with one call, so a page of 50 users costs one query.
- **No thundering herd.** Concurrent misses for the same key share a single load.
- **Freshness controls.** TTL with jitter, caching of "not found" answers, and stale-while-revalidate.
- **LRU, LFU or FIFO eviction**, chosen with one config field.
- **Plain configuration.** A validated `Config` struct; only `Capacity` is required.
- **Fast.** A cache hit takes 16 ns, writes do not allocate, and `Shards` spreads concurrent load over several locks.

<p align="center">
  <img src="https://images.luminyra.dev/luminyra/github/memo/cover.webp" alt="A golden key rests in an olive cache slot connected to request tags." width="640">
</p>

## Contents

- [When to use it](#when-to-use-it) · [Install](#install) · [Getting started](#getting-started)
- [Common tasks](#common-tasks) · [How it works](#how-it-works) · [Performance](#performance) · [API at a glance](#api-at-a-glance)

## When to use it

memo is a good fit when the same values are read far more often than they change, for example:

- user profiles, permissions or feature flags looked up on every request,
- configuration or exchange rates fetched from another service,
- expensive computations whose results can be reused for a few minutes.

It is an **in-process** cache: each instance of your program has its own copy. If several servers must share one cache, or data must survive a restart, use Redis or a similar store instead.

## Install

```sh
go get github.com/luminyra/memo
```

memo requires Go 1.27 or later. Its only dependency is [conc](https://github.com/luminyra/conc), another Luminyra library with no dependencies.

## Getting started

The program below caches users that would normally come from a database. Copy it into `main.go` and run it with `go run .`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/luminyra/memo"
)

type User struct {
	ID   int
	Name string
}

// loadUsers stands in for a query such as
// SELECT id, name FROM users WHERE id IN (...).
func loadUsers(ctx context.Context, ids []int) (map[int]User, error) {
	fmt.Println("query database:", ids)
	users := make(map[int]User, len(ids))
	for _, id := range ids {
		users[id] = User{ID: id, Name: fmt.Sprintf("user-%d", id)}
	}
	return users, nil
}

func main() {
	users, err := memo.New(memo.Config[int, User]{
		Capacity: 10_000,          // keep at most 10,000 users in memory
		TTL:      5 * time.Minute, // reload a user after five minutes
		Loader:   loadUsers,       // called on a cache miss
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for range 3 {
		user, found, err := users.Get(ctx, 42)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(user.Name, found)
	}
}
```

The database is queried once; the next two calls are answered from memory:

```text
query database: [42]
user-42 true
user-42 true
user-42 true
```

Three things to notice:

1. **The loader takes a slice of keys** and returns a map. A key missing from the map means "this user does not exist", and `Get` then reports `found == false`.
2. **`Get` returns `(value, found, err)`.** `err` is the loader's error, or the context's error if you stop waiting.
3. **Only `Capacity` is required.** Every other `Config` field is optional, and its zero value turns the feature off. `New` checks the whole configuration and returns an error if something does not make sense.

<p align="center">
  <a href="https://images.luminyra.dev/luminyra/github/memo/demo.webp"><img src="https://images.luminyra.dev/luminyra/github/memo/demo.webp" alt="Ten goroutines read the same uncached key. One loader call provides the result, which later reads can retrieve from memory." width="760"></a>
</p>

## Common tasks

### Load many keys with one query

`GetMany` returns the cached values immediately and loads all the missing keys with **a single** loader call, so a page that shows 50 users costs at most one database round trip:

```go
found, err := users.GetMany(ctx, []int{1, 2, 3, 404})
// found has an entry for every user that exists; 404 is simply absent.
```

### Stop asking for keys that do not exist

Without help, a request for a user that does not exist reaches the database every time. `CacheMissing` remembers "not found" answers until their TTL expires:

```go
memo.Config[int, User]{Capacity: 10_000, TTL: time.Minute, Loader: loadUsers, CacheMissing: true}
```

### Keep serving while data refreshes

With `StaleWhileRevalidate`, an expired value is still returned for a grace period while memo reloads it in the background, so readers never wait for the database once a value has been cached:

```go
memo.Config[int, User]{Capacity: 10_000, TTL: time.Minute, Loader: loadUsers, StaleWhileRevalidate: 30 * time.Second}
```

### Spread out expirations

Entries written at the same moment, for example at startup, would also expire at the same moment and hit the database together. `Jitter` shortens each TTL by a random fraction:

```go
memo.Config[int, User]{Capacity: 10_000, TTL: time.Minute, Jitter: 0.1, Loader: loadUsers} // TTLs between 54s and 60s
```

### Scale reads across cores

One lock protects each shard. When many goroutines use the cache at once, split it:

```go
memo.Config[int, User]{Capacity: 100_000, Loader: loadUsers, Shards: runtime.GOMAXPROCS(0)}
```

Keys are spread over the shards by hash. Each shard holds `Capacity/Shards` entries and evicts on its own, so eviction order is exact within a shard only.

### Choose what to evict when the cache is full

| Policy | Evicts | Good for |
| --- | --- | --- |
| `memo.LRU` (default) | the entry read least recently | most workloads |
| `memo.LFU` | the entry read least often | a stable set of popular keys |
| `memo.FIFO` | the oldest entry | data that simply ages out |

### Use it without a loader

memo also works as a plain key-value cache. Without a `Loader`, `Get` reports `found == false` on a miss and you fill the cache yourself:

```go
sessions, _ := memo.New(memo.Config[string, Session]{Capacity: 1_000, TTL: 30 * time.Minute})
sessions.Set(token, Session{UserID: 42})
s, found, _ := sessions.Get(ctx, token)
```

### Test code that depends on time

memo reads the clock through the `time` package, so the standard `testing/synctest` package can move time forward in tests without sleeping:

```go
synctest.Test(t, func(t *testing.T) {
	cache.Set("k", "v")
	time.Sleep(2 * time.Minute) // instant inside the bubble
	if cache.Has("k") {
		t.Fatal("entry should have expired")
	}
})
```

## How it works

- A `Cache` is safe for concurrent use. Concurrent misses for the same key wait for **one** loader call, and a caller that gives up (its context ends) does not cancel the load for the others.
- The loader runs with a background context, detached from any single request. A miss whose context has already ended returns `context.Cause(ctx)` without starting a load; cached values are still returned.
- Every load finishes its waiters however the loader ends. A loader error is returned to each of them; a loader that calls `runtime.Goexit` fails the load with `conc.ErrGoexit`; a loader panic re-panics in the waiting callers as a `conc.Panic` with the original stack, or in the loading goroutine if nobody is waiting.
- `MaxConcurrentLoads` caps the number of loader calls running at once. Loads beyond the cap queue in arrival order, and their callers can still give up through their contexts.
- Expired entries are removed lazily, when they are read or evicted, so `Len` may briefly include them.
- `Peek`, `Has` and `All` read the cache without loading and without changing the eviction order.
- One mutex protects each shard, one shard by default. Under heavy concurrent use, set `Shards`; see [Performance](#performance).

## Performance

Measured with the benchmarks in this repository (`make bench`; Go 1.27.4, linux/arm64, 5 CPUs, median of 6 runs). Run them on your hardware for numbers that matter to you.

| Operation | Time | Allocations |
| --- | --- | --- |
| `Get`, cache hit | 16 ns | 0 |
| `GetMany`, 64 keys, all hits | 1.7 µs (26 ns per key) | 4 |
| `Set` into a full cache, LRU or FIFO eviction | 43 ns | 0 |
| `Set` into a full cache, LFU eviction | 109 ns | 0 |
| `Get` from 5 goroutines at once, one shard | 97 ns per call | 0 |
| `Get` from 5 goroutines at once, `Shards: 16` | 68 ns per call | 0 |

Entries live in one slice and link to each other by index, so writes and evictions reuse memory instead of allocating. A hit without a TTL does not even read the clock. Under concurrent load, a shard's lock is the bottleneck: use `Shards`, and note that LRU and LFU record every hit, while FIFO reads leave the eviction order untouched and scale best.

## API at a glance

| Task | API |
| --- | --- |
| Create | `New(Config)` |
| Read through the loader | `Get`, `GetMany` |
| Read without loading | `Peek`, `Has`, `All`, `Len` |
| Write and remove | `Set`, `SetWithTTL`, `Delete`, `Purge` |

The [Go package reference](https://pkg.go.dev/github.com/luminyra/memo) documents the API. The programs in [`examples/`](examples) show [batched and coalesced loads](examples/loader/main.go), [eviction policies](examples/policy/main.go), and [concurrent writes to a sharded cache](examples/shards/main.go).

## Versioning

memo follows [semantic versioning](https://semver.org). Until v1.0.0, a minor release may change the API; every change is listed in the [changelog](CHANGELOG.md). It supports the two most recent Go releases, starting with Go 1.27.

## Contributing

Questions, bug reports and pull requests are welcome. See the [contributing guide](https://github.com/luminyra/.github/blob/main/CONTRIBUTING.md), and report security issues as described in the [security policy](https://github.com/luminyra/.github/blob/main/SECURITY.md).

memo was inspired by [samber/hot](https://github.com/samber/hot) and the HTTP `stale-while-revalidate` directive. It is released under the [MIT License](LICENSE).
