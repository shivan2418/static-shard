// Shared "build -> deploy -> query in a browser" e2e harness used by every example's test/e2e.test.ts.
// "Deploy" = serve the real `vite build` output (a plain static file tree, exactly what a static
// host would serve) over HTTP; "query in a browser" = drive it with a real headless Chromium.
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json",
  ".ndjson": "application/x-ndjson",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
};

/** Serves `rootDir` (a static build's `dist/`) over plain HTTP on an ephemeral localhost port. */
export function serveStatic(rootDir) {
  const server = createServer((req, res) => {
    void (async () => {
      try {
        const requestedPath = decodeURIComponent(new URL(req.url ?? "/", "http://localhost").pathname);
        const filePath = path.join(rootDir, requestedPath === "/" ? "/index.html" : requestedPath);
        if (!filePath.startsWith(rootDir)) {
          res.writeHead(403).end("forbidden");
          return;
        }
        const stats = await stat(filePath).catch(() => null);
        if (!stats || stats.isDirectory()) {
          res.writeHead(404).end("not found");
          return;
        }
        const body = await readFile(filePath);
        res.writeHead(200, { "content-type": MIME_TYPES[path.extname(filePath)] ?? "application/octet-stream" });
        res.end(body);
      } catch (err) {
        res.writeHead(500).end(String(err));
      }
    })();
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => server.close(() => r(undefined))),
      });
    });
  });
}

/** Opens a real headless-Chromium page against `url`, runs `fn(page)`, then always closes the browser. */
export async function withPage(url, fn) {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(url);
    return await fn(page);
  } finally {
    await browser.close();
  }
}
