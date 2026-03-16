#!/usr/bin/env node
// Headless Chrome test runner using CDP (Chrome DevTools Protocol)
// Polls the page title until it becomes PASS or FAIL

const http = require("http");
const { execSync, spawn } = require("child_process");

const PORT = 8787;
const TIMEOUT = 60000; // 60 seconds
const TEST_URL = `http://localhost:${PORT}/`;

// Find Chrome
const CHROME_CANDIDATES = [
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "google-chrome",
  "google-chrome-stable",
  "chromium",
  "chromium-browser",
];

let chromePath = null;
for (const c of CHROME_CANDIDATES) {
  try {
    if (require("fs").existsSync(c)) { chromePath = c; break; }
    execSync(`which "${c}"`, { stdio: "ignore" });
    chromePath = c;
    break;
  } catch {}
}

if (!chromePath) {
  console.error("Chrome not found");
  process.exit(1);
}

// Launch Chrome with remote debugging
const tmpDir = require("os").tmpdir() + "/opfs-test-chrome-" + Date.now();
require("fs").mkdirSync(tmpDir, { recursive: true });

const chrome = spawn(chromePath, [
  "--headless=new",
  "--disable-gpu",
  "--no-sandbox",
  `--user-data-dir=${tmpDir}`,
  "--remote-debugging-port=9222",
  TEST_URL,
], { stdio: ["ignore", "pipe", "pipe"] });

let chromeOutput = "";
chrome.stderr.on("data", (d) => { chromeOutput += d.toString(); });
chrome.stdout.on("data", (d) => { chromeOutput += d.toString(); });

// Wait for Chrome to start, then poll via CDP
setTimeout(async () => {
  const startTime = Date.now();

  while (Date.now() - startTime < TIMEOUT) {
    try {
      // Get the list of pages via CDP
      const data = await httpGet("http://localhost:9222/json");
      const pages = JSON.parse(data);
      const page = pages.find(p => p.url.includes("localhost:" + PORT));

      if (page) {
        // Connect to the page's WebSocket and evaluate JS
        const wsUrl = page.webSocketDebuggerUrl;
        if (wsUrl) {
          const result = await evaluateViaWs(wsUrl, "document.title");
          if (result === "PASS" || result === "FAIL") {
            // Get the full page content for the log
            const bodyText = await evaluateViaWs(wsUrl, "document.body.innerText");
            console.log("\n=== OPFS Test Results ===\n");
            console.log(bodyText);
            console.log("\n=== Result: " + result + " ===\n");

            chrome.kill();
            require("fs").rmSync(tmpDir, { recursive: true, force: true });
            process.exit(result === "PASS" ? 0 : 1);
          }
        }
      }
    } catch (e) {
      // CDP not ready yet, retry
    }
    await sleep(1000);
  }

  console.error("TIMEOUT: Tests did not complete within " + (TIMEOUT / 1000) + "s");
  chrome.kill();
  require("fs").rmSync(tmpDir, { recursive: true, force: true });
  process.exit(1);
}, 2000);

function httpGet(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = "";
      res.on("data", (chunk) => data += chunk);
      res.on("end", () => resolve(data));
    }).on("error", reject);
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function evaluateViaWs(wsUrl, expression) {
  return new Promise((resolve, reject) => {
    const WebSocket = require("ws");
    const ws = new WebSocket(wsUrl);
    const id = 1;

    ws.on("open", () => {
      ws.send(JSON.stringify({
        id,
        method: "Runtime.evaluate",
        params: { expression, returnByValue: true }
      }));
    });

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.id === id) {
          ws.close();
          if (msg.result && msg.result.result) {
            resolve(msg.result.result.value);
          } else {
            resolve(undefined);
          }
        }
      } catch (e) {
        ws.close();
        reject(e);
      }
    });

    ws.on("error", reject);
    setTimeout(() => { ws.close(); reject(new Error("WS timeout")); }, 5000);
  });
}
