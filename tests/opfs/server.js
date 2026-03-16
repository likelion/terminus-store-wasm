#!/usr/bin/env node
// Simple static file server with Cross-Origin-Isolation headers.
// Required for SharedArrayBuffer support in the OPFS test harness.

const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = parseInt(process.env.PORT || "8787", 10);
const ROOT = path.resolve(__dirname, "../..");

const MIME_TYPES = {
  ".html": "text/html",
  ".js": "application/javascript",
  ".mjs": "application/javascript",
  ".wasm": "application/wasm",
  ".json": "application/json",
  ".css": "text/css",
};

const server = http.createServer((req, res) => {
  // Set Cross-Origin-Isolation headers on ALL responses
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");

  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/tests/opfs/index.html";

  const filePath = path.join(ROOT, urlPath);

  // Prevent directory traversal
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not Found: " + urlPath);
      return;
    }
    const ext = path.extname(filePath);
    const mime = MIME_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, { "Content-Type": mime });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`OPFS test server listening on http://localhost:${PORT}`);
});
