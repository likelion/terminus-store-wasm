#!/usr/bin/env node
// Run OPFS tests in headless Chrome via Puppeteer.
// Requires the test server to be running on PORT (default 8787).
const puppeteer = require("puppeteer");

const PORT = parseInt(process.env.PORT || "8787", 10);
const TIMEOUT = 120000;

async function main() {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();

  // Capture console output from the page
  page.on("console", (msg) => {
    console.log(`[page console.${msg.type()}] ${msg.text()}`);
  });

  page.on("pageerror", (err) => {
    console.log(`[page error] ${err.message}`);
  });

  // Capture network failures
  page.on("requestfailed", (req) => {
    console.log(`[network FAIL] ${req.url()} — ${req.failure().errorText}`);
  });

  page.on("response", (res) => {
    if (res.status() >= 400) {
      console.log(`[network ${res.status()}] ${res.url()}`);
    }
  });

  console.log(`Navigating to http://localhost:${PORT}/`);
  await page.goto(`http://localhost:${PORT}/`, { waitUntil: "domcontentloaded" });

  console.log("Waiting for tests to complete...");

  // Wait for document.title to become PASS or FAIL
  try {
    await page.waitForFunction(
      () => document.title === "PASS" || document.title === "FAIL",
      { timeout: TIMEOUT }
    );
  } catch (e) {
    const title = await page.title();
    const body = await page.evaluate(() => document.body.innerText);
    console.error(`\nTIMEOUT (title=${title})`);
    console.error("Page body:\n" + body);
    await browser.close();
    process.exit(1);
  }

  const title = await page.title();
  const body = await page.evaluate(() => document.body.innerText);

  console.log("\n=== OPFS Test Results ===\n");
  console.log(body);
  console.log("\n=== Result: " + title + " ===");

  await browser.close();
  process.exit(title === "PASS" ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
