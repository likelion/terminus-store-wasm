# terminus-store-wasm Makefile
# ──────────────────────────────────────────────────────────────────
# Targets:
#   build          – release WASM build (wasm-pack + wasm-opt)
#   build-dev      – debug WASM build (fast iteration)
#   build-opt      – release build + wasm-opt -Oz
#   test           – native unit tests (both crates)
#   test-wasm      – wasm-pack test in headless Chrome
#   test-opfs      – OPFS integration tests in headless Chrome
#   test-all       – all of the above
#   check          – cargo check (wasm32 target)
#   clippy         – cargo clippy (wasm32 target)
#   clean          – remove build artifacts
#   size           – show WASM binary sizes (raw + gzip)
#   help           – show this help
# ──────────────────────────────────────────────────────────────────

SHELL       := /bin/bash
PKG_DIR     := pkg
WASM_BIN    := $(PKG_DIR)/terminus_store_wasm_bg.wasm
WASM_OPT    := $(PKG_DIR)/terminus_store_wasm_bg.wasm
TARGET      := wasm32-unknown-unknown
OPFS_PORT   ?= 8787
SUCCINCT    := ../tdb-succinct-wasm

.PHONY: all build build-dev build-opt test test-native test-wasm test-opfs \
        test-all check clippy clean size help

all: build

# ── Build ─────────────────────────────────────────────────────────

build:
	wasm-pack build --target web --release
	@echo "── built $(WASM_BIN)"
	@ls -lh $(WASM_BIN)

build-dev:
	wasm-pack build --target web --dev
	@echo "── built $(WASM_BIN) (dev)"
	@ls -lh $(WASM_BIN)

build-opt: build
	wasm-opt -Oz --enable-bulk-memory $(WASM_BIN) -o $(WASM_BIN)
	@echo "── optimised $(WASM_BIN)"
	@ls -lh $(WASM_BIN)

# ── Tests ─────────────────────────────────────────────────────────

test: test-native

test-native:
	@echo "── tdb-succinct-wasm native tests"
	cargo test -p tdb-succinct-wasm
	@echo "── terminus-store-wasm native tests"
	cargo test -p terminus-store-wasm

test-wasm: build
	wasm-pack test --headless --chrome

test-opfs: build
	@echo "── OPFS integration tests (port $(OPFS_PORT))"
	PORT=$(OPFS_PORT) bash tests/opfs/run.sh --no-build

test-all: test-native test-wasm test-opfs

# ── Lint / Check ──────────────────────────────────────────────────

check:
	cargo check --target $(TARGET)
	cargo check -p tdb-succinct-wasm

clippy:
	cargo clippy --target $(TARGET) -- -D warnings
	cargo clippy -p tdb-succinct-wasm -- -D warnings

# ── Utilities ─────────────────────────────────────────────────────

size: build-opt
	@echo ""
	@echo "── WASM binary sizes ──"
	@printf "  raw:    %s\n" "$$(ls -lh $(WASM_BIN) | awk '{print $$5}')"
	@printf "  gzip:   %s\n" "$$(gzip -c $(WASM_BIN) | wc -c | awk '{printf "%.0fK", $$1/1024}')"
	@printf "  brotli: %s\n" "$$(if command -v brotli >/dev/null 2>&1; then brotli -c $(WASM_BIN) | wc -c | awk '{printf "%.0fK", $$1/1024}'; else echo 'n/a (install brotli)'; fi)"

clean:
	cargo clean
	rm -rf $(PKG_DIR)
	@echo "── cleaned"

help:
	@head -16 Makefile | tail -14
