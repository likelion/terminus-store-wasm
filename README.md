# terminus-store-wasm, a synchronous data store for triple data

[![Build Status](https://github.com/likelion/terminus-store-wasm/workflows/Build/badge.svg)](https://github.com/likelion/terminus-store-wasm/actions)
[![codecov](https://codecov.io/github/likelion/terminus-store-wasm/branch/main/graph/badge.svg)](https://app.codecov.io/github/likelion/terminus-store-wasm)

## Overview
This library implements a way to store triple data - data that
consists of a subject, predicate and an object, where object can
either be some value, or a node (a string that can appear both in
subject and object position).

An example of triple data is:
````
cow says value(moo).
duck says value(quack).
cow likes node(duck).
duck hates node(cow).
````
In `cow says value(moo)`, `cow` is the subject, `says` is the
predicate, and `value(moo)` is the object.

In `cow likes node(duck)`, `cow` is the subject, `likes` is the
predicate, and `node(duck)` is the object.

terminus-store-wasm allows you to store a lot of such facts, and
search through them efficiently.

This is a synchronous, WASM-compatible fork of
[terminus-store](https://github.com/terminusdb/terminusdb-store). It
replaces the tokio async runtime with a fully synchronous API, making
it suitable for compilation to WebAssembly (both browser and Node.js
targets) as well as native use.

### Storage backends
- **Memory** — in-process, no persistence
- **Filesystem** — native targets via `std::fs`
- **OPFS** — browser targets via the Origin Private File System API

Layer data is binary-compatible with the original terminus-store, so
databases created by either version can be read by the other.

## Usage
Add the dependency pointing at this repository:

```toml
[dependencies]
terminus-store-wasm = { git = "https://github.com/likelion/terminus-store-wasm" }
```

Open a memory-backed store:
```rust
use terminus_store_wasm::open_memory_store;

let store = open_memory_store();
```

Or a directory-backed store (native only):
```rust
use terminus_store_wasm::open_directory_store;

let store = open_directory_store("/path/to/store").unwrap();
```

## License
terminus-store is licensed under Apache 2.0.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)

## See also
- The original async terminus-store: [GitHub](https://github.com/terminusdb/terminusdb-store)
- The Terminus database: [Website](https://terminusdb.com) — [GitHub](https://github.com/terminusdb/)
- The HDT format, which the layer format is based on: [Website](http://www.rdfhdt.org/)
