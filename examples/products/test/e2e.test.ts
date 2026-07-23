import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { build as viteBuild } from "vite";
import { serveStatic, withPage } from "../../shared/harness.mjs";
import { run as buildData } from "../scripts/build-data.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

let server: Awaited<ReturnType<typeof serveStatic>>;

describe("products example: build -> deploy -> query in a browser", () => {
  beforeAll(async () => {
    buildData();
    await viteBuild({ root, logLevel: "warn", build: { emptyOutDir: true } });
    server = await serveStatic(path.join(root, "dist"));
  }, 60_000);

  afterAll(async () => {
    await server.close();
  });

  test("get(id) looks up the seed SKU in a real headless browser", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForFunction(() => (document.getElementById("lookup-result")?.textContent ?? "").length > 0);
      const text = await page.textContent("#lookup-result");
      expect(text).toContain("Rustic Kettle");
      expect(text).toContain("42.5");
      expect(text).toContain("in stock");
    });
  });

  test("get(id) reports a miss for an unknown SKU", async () => {
    await withPage(server.url, async (page) => {
      await page.fill("#lookup-input", "SKU-NOT-REAL");
      await page.click("#lookup-form button");
      await page.waitForFunction(() => (document.getElementById("lookup-result")?.textContent ?? "").includes("no product"));
      const text = await page.textContent("#lookup-result");
      expect(text).toContain('no product with SKU "SKU-NOT-REAL"');
    });
  });

  test("boolean equals + exists filters return only in-stock, discounted products", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForSelector('[data-testid="discounted-results"] li');
      const items = await page.$$eval('[data-testid="discounted-results"] li', (nodes) =>
        nodes.map((n) => n.textContent ?? ""),
      );
      expect(items.length).toBeGreaterThan(0);
      expect(items.length).toBeLessThanOrEqual(10);
      for (const text of items) {
        expect(text).toMatch(/% off/);
      }
      const prices = items.map((text) => Number(/\$([\d.]+)/.exec(text)?.[1]));
      expect(prices).toEqual([...prices].sort((a, b) => a - b));
    });
  });

  test("count() reports the full catalog size", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForFunction(() => (document.getElementById("total-count")?.textContent ?? "").includes("products"));
      const text = await page.textContent("#total-count");
      expect(text).toContain("2000 products");
    });
  });
});
