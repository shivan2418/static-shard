#!/usr/bin/env node
// Deterministic sample dataset for the movies example — regenerate with `pnpm run generate-data`.
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { mulberry32 } from "../../shared/prng.mjs";

const outFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../data/movies.ndjson");

const RECORD_COUNT = 3000;

const rand = mulberry32(0x5eed1234);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const int = (min, max) => min + Math.floor(rand() * (max - min + 1));
const round1 = (n) => Math.round(n * 10) / 10;

const ADJECTIVES = [
  "Silent", "Crimson", "Broken", "Endless", "Hidden", "Golden", "Last", "Forgotten",
  "Electric", "Quiet", "Wild", "Distant", "Sacred", "Bitter", "Radiant", "Lonely",
  "Restless", "Faded", "Burning", "Frozen", "Secret", "Painted", "Ancient", "Drifting",
];
const NOUNS = [
  "Horizon", "River", "Kingdom", "Machine", "Garden", "Storm", "Harbor", "Mirror",
  "Voyage", "Ember", "Labyrinth", "Orchard", "Signal", "Wolf", "Ocean", "Meridian",
  "Archive", "Carousel", "Lantern", "Threshold", "Wanderer", "Cathedral", "Static", "Compass",
];
const FIRST_NAMES = [
  "Jordan", "Alex", "Morgan", "Casey", "Riley", "Quinn", "Sasha", "Reese",
  "Drew", "Emerson", "Harper", "Rowan", "Skyler", "Toni", "Wren", "Blair",
  "Marlowe", "Ellis", "Frankie", "Shiloh",
];
const LAST_NAMES = [
  "Blake", "Reyes", "Nakamura", "O'Connell", "Petrov", "Adeyemi", "Kowalski", "Lindgren",
  "Okafor", "Delacroix", "Whitfield", "Sundberg", "Marchetti", "Osei", "Vance", "Iyer",
  "Hollis", "Farrow", "Novak", "Beaumont",
];
const GENRES = [
  "Drama", "Comedy", "Action", "Thriller", "Romance", "Sci-Fi", "Horror",
  "Documentary", "Animation", "Mystery", "Adventure", "Fantasy", "Crime", "Musical", "War",
];
const OVERVIEW_TEMPLATES = [
  (a, b) => `A reclusive ${a} uncovers a decades-old secret that threatens to unravel ${b}'s history.`,
  (a, b) => `When ${a} goes missing, a small community in ${b} is forced to confront its past.`,
  (a, b) => `Two rival ${a}s form an uneasy alliance to survive a single night in ${b}.`,
  (a, b) => `A retired ${a} is pulled back into one last job that could redeem — or destroy — ${b}.`,
  (a, b) => `Set against the backdrop of ${b}, a young ${a} learns what it truly costs to belong.`,
];
const OVERVIEW_SUBJECTS = [
  "lighthouse keeper", "detective", "cartographer", "musician", "surgeon", "pilot",
  "archivist", "smuggler", "diplomat", "engineer", "photographer", "chef",
];
const OVERVIEW_PLACES = [
  "the coastal town", "the capital", "a border province", "the old quarter",
  "a mountain outpost", "the harbor district", "an island commune", "the valley",
];

function directorName() {
  return `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)}`;
}

// A fixed set of directors (not one-per-record) so equals/in queries return a realistic multi-row spread.
const DIRECTORS = Array.from({ length: 40 }, () => directorName());

function titleFor(index) {
  return `${pick(ADJECTIVES)} ${pick(NOUNS)}${index % 7 === 0 ? "s" : ""}`;
}

function genresFor() {
  const count = int(1, 3);
  const chosen = new Set();
  while (chosen.size < count) chosen.add(pick(GENRES));
  return [...chosen];
}

function overviewFor() {
  const template = pick(OVERVIEW_TEMPLATES);
  return template(pick(OVERVIEW_SUBJECTS), pick(OVERVIEW_PLACES));
}

const records = [];

// Record 0 is a fixed, hand-authored "seed" record with known values — the examples' e2e test and
// UI default query assert against this exact record rather than anything procedurally generated.
records.push({
  id: "m-0000",
  title: "The Silent Horizon",
  year: 1994,
  rating: 8.7,
  director: "Jordan Blake",
  genres: ["Drama", "Mystery"],
  overview:
    "A reclusive lighthouse keeper uncovers a decades-old secret that threatens to unravel the coastal town's history.",
});

for (let i = 1; i < RECORD_COUNT; i++) {
  records.push({
    id: `m-${String(i).padStart(4, "0")}`,
    title: titleFor(i),
    year: int(1950, 2026),
    rating: round1(int(10, 100) / 10),
    director: pick(DIRECTORS),
    genres: genresFor(),
    overview: overviewFor(),
  });
}

writeFileSync(outFile, records.map((r) => JSON.stringify(r)).join("\n") + "\n");
console.log(`[generate-data] wrote ${records.length} records to ${path.relative(process.cwd(), outFile)}`);
