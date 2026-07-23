#!/usr/bin/env node
// Deterministic sample dataset for the products example — regenerate with `pnpm run generate-data`.
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { mulberry32 } from "../../shared/prng.mjs";

const outFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../data/products.csv");

const RECORD_COUNT = 2000;

const rand = mulberry32(0xc0ffee);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const int = (min, max) => min + Math.floor(rand() * (max - min + 1));
const round2 = (n) => Math.round(n * 100) / 100;

const CATEGORIES = ["Kitchen", "Outdoors", "Office", "Electronics", "Fitness", "Garden", "Toys", "Stationery"];
const ADJECTIVES = ["Compact", "Deluxe", "Portable", "Rustic", "Modular", "Ergonomic", "Classic", "Rapid", "Ultra", "Everyday"];
const NOUNS = ["Kettle", "Backpack", "Desk Lamp", "Speaker", "Yoga Mat", "Trowel", "Puzzle", "Notebook", "Grinder", "Tent"];

function csvField(value) {
  if (value === undefined) return "";
  const s = String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function nameFor() {
  return `${pick(ADJECTIVES)} ${pick(NOUNS)}`;
}

const rows = [];

// A fixed, hand-authored "seed" row — the e2e test asserts `get(id)` against this exact SKU.
rows.push({
  sku: "SKU-00001",
  name: "Rustic Kettle",
  category: "Kitchen",
  price: 42.5,
  inStock: true,
  discountPct: 15,
});

for (let i = 1; i < RECORD_COUNT; i++) {
  const hasDiscount = rand() < 0.3;
  rows.push({
    sku: `SKU-${String(i + 1).padStart(5, "0")}`,
    name: nameFor(),
    category: pick(CATEGORIES),
    price: round2(int(500, 25000) / 100),
    inStock: rand() < 0.85,
    discountPct: hasDiscount ? int(5, 50) : undefined,
  });
}

const header = "sku,name,category,price,inStock,discountPct";
const lines = rows.map((r) =>
  [csvField(r.sku), csvField(r.name), csvField(r.category), csvField(r.price), csvField(r.inStock), csvField(r.discountPct)].join(","),
);

writeFileSync(outFile, [header, ...lines].join("\n") + "\n");
console.log(`[generate-data] wrote ${rows.length} records to ${path.relative(process.cwd(), outFile)}`);
