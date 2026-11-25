# memo

[![Go Reference](https://pkg.go.dev/badge/github.com/luminyra/memo.svg)](https://pkg.go.dev/github.com/luminyra/memo)
[![CI](https://github.com/luminyra/memo/actions/workflows/ci.yml/badge.svg)](https://github.com/luminyra/memo/actions/workflows/ci.yml)

An in-memory cache for Go 1.27 with LRU, LFU and FIFO eviction, TTL with jitter, batch loaders, missing-key caching and stale-while-revalidate.

```go
users, err := memo.New(memo.Config[int, User]{
	Capacity: 10_000,
	TTL:      time.Minute,
	Loader:   loadUsers, // func(ctx context.Context, ids []int) (map[int]User, error)
})

user, found, err := users.Get(ctx, 42)
batch, err := users.GetMany(ctx, []int{1, 2, 3}) // misses load in one call
```

## Install

```sh
go get github.com/luminyra/memo
```

Requires Go 1.27 or later. The module has no dependencies.

## Highlights

- **A plain config struct.** Only `Capacity` is required; the zero value of every other field turns its feature off, and `New` validates the whole configuration at once.
- **Batch loading.** A `Loader` takes a slice of keys. `GetMany` loads all of its misses with a single call, so a database-backed loader can run one `IN` query.
- **No thundering herd.** Concurrent misses for the same key share one load, and a canceled caller does not cancel the load for others.
- **Freshness controls.** `TTL` with `Jitter` spreads expirations, `CacheMissing` remembers keys the loader did not return, and `StaleWhileRevalidate` serves an expired value while it reloads in the background.
- **Testable time.** The cache uses the `time` package directly, so `testing/synctest` controls expiration in tests without a fake clock.

## Usage

```go
cache, err := memo.New(memo.Config[string, []byte]{
	Capacity:             1_000,
	Policy:               memo.LFU,
	TTL:                  5 * time.Minute,
	Jitter:               0.1,
	Loader:               fetch,
	CacheMissing:         true,
	StaleWhileRevalidate: time.Minute,
})

cache.Set("motd", []byte("hello"))
for key, value := range cache.All() { // a snapshot of the fresh entries
	fmt.Println(key, len(value))
}
```

## Examples

Runnable programs live in [`examples/`](examples):

| Example | Shows |
| --- | --- |
| [`loader`](examples/loader/main.go) | coalesced loads, `GetMany` batching, missing-key caching |
| [`policy`](examples/policy/main.go) | which entries LRU, LFU and FIFO keep |

```sh
go run ./examples/loader
```

## Design

memo is inspired by [samber/hot](https://github.com/samber/hot) and the HTTP `stale-while-revalidate` cache directive. It targets Go 1.27 only and favors a validated config struct over a builder, batch-first loaders, and iterator access to its contents.

## License

[MIT](LICENSE)
