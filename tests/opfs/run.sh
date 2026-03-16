#!/usr/bin/env bash
# Run OPFS integration tests in headless Chrome.
# Usage: ./tests/opfs/run.sh [--no-build]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

# Build unless --no-build is passed
if [[ "$1" != "--no-build" ]]; then
  echo "==> Building WASM module..."
  wasm-pack build --target web --out-dir pkg
fi

# Start test server in background
echo "==> Starting test server..."
node tests/opfs/server.js &
SERVER_PID=$!

cleanup() {
  kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
for i in $(seq 1 20); do
  if curl -s -o /dev/null http://localhost:8787/ 2>/dev/null; then
    break
  fi
  sleep 0.25
done

# Run tests
echo "==> Running OPFS tests..."
node tests/opfs/run_tests.js
