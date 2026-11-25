# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- In-memory cache with LRU, LFU and FIFO eviction, TTL with jitter, batch loaders, `GetMany`, missing-key caching and stale-while-revalidate.
- Requires Go 1.27 or later.
