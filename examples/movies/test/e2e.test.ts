import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { build as viteBuild } from "vite";
import { serveStatic, withPage } from "../../shared/harness.mjs";
import { run as buildData } from "../scripts/build-data.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

let server: Awaited<ReturnType<typeof serveStatic>>;

describe("movies example: build -> deploy -> query in a browser", () => {
  beforeAll(async () => {
    buildData();
    await viteBuild({ root, logLevel: "warn", build: { emptyOutDir: true } });
    server = await serveStatic(path.join(root, "dist"));
  }, 60_000);

  afterAll(async () => {
    await server.close();
  });

  test("title `contains` search finds the seed record in a real headless browser", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForSelector('[data-testid="search-results"] li');
      const text = await page.textContent('[data-testid="search-results"]');
      expect(text).toContain("The Silent Horizon");
      expect(text).toContain("1994");
      expect(text).toContain("Jordan Blake");
    });
  });

  test("year range + orderBy on a secondary field returns a correctly filtered, correctly sorted page", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForSelector('[data-testid="filtered-results"] li');
      const items = await page.$$eval('[data-testid="filtered-results"] li', (nodes) =>
        nodes.map((n) => n.textContent ?? ""),
      );
      expect(items.length).toBeGreaterThan(0);
      expect(items.length).toBeLessThanOrEqual(10);

      const parsed = items.map((text) => {
        const yearMatch = /\((\d{4})\)/.exec(text);
        const ratingMatch = /— ([\d.]+)\/10/.exec(text);
        return { year: Number(yearMatch?.[1]), rating: Number(ratingMatch?.[1]) };
      });
      for (const { year } of parsed) {
        expect(year).toBeGreaterThanOrEqual(2015);
        expect(year).toBeLessThanOrEqual(2020);
      }
      const ratings = parsed.map((p) => p.rating);
      expect(ratings).toEqual([...ratings].sort((a, b) => b - a));
    });
  });

  test("count() reports the full catalog size", async () => {
    await withPage(server.url, async (page) => {
      await page.waitForFunction(() => (document.getElementById("total-count")?.textContent ?? "").includes("movies"));
      const text = await page.textContent("#total-count");
      expect(text).toContain("3000 movies");
    });
  });
});
