# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- In-memory cache with LRU, LFU and FIFO eviction, TTL with jitter, batch loaders, `GetMany`, missing-key caching and stale-while-revalidate.
- `Config.MaxConcurrentLoads` caps the number of loader calls running at once.
- Every load finishes its waiters however the loader ends: a loader that calls `runtime.Goexit` fails the load with `conc.ErrGoexit`, and a loader panic re-panics in the waiting callers as a `conc.Panic`, or in the loading goroutine if nobody waits.
- A miss whose context has already ended returns `context.Cause(ctx)` without starting a load, and a caller that stops waiting gets `context.Cause(ctx)`.
- Depends on `github.com/luminyra/conc`.
- `Config.Shards` splits a cache into independently locked parts for concurrent workloads.
- Entries live in one slice linked by index: writes and evictions do not allocate, and a cache hit takes 16 ns.
- Benchmarks (`make bench`).
- A Makefile with the development tasks (`make help`).
- Requires Go 1.27 or later.
