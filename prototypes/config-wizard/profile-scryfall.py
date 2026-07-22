#!/usr/bin/env python3
# PROTOTYPE helper — regenerate the numbers in profile.ts from the raw file.
#   gzip -dc default-cards-*.jsonl.gz | python3 profile-scryfall.py
# Mirrors what static-shard `init` does: stream a sample, infer per-field type,
# cardinality, avg value bytes, presence (absent-vs-null), and the p95 record
# size, then scale by the observed gzip ratio. Not production code.
import sys, json, os
from collections import defaultdict

FIELDS = {
    "id": ("string", False), "name": ("string", False),
    "released_at": ("date", False), "cmc": ("number", False),
    "rarity": ("string", False), "set": ("string", False),
    "artist": ("string", False), "type_line": ("string", False),
    "colors": ("string", True), "keywords": ("string", True),
    "reprint": ("boolean", False), "oracle_text": ("string", False),
}
COMPRESSED = os.environ.get("SRC", "/home/emil/Downloads/default-cards-20260721211623.jsonl.gz")

distinct = defaultdict(set); sumlen = defaultdict(int)
count = defaultdict(int); present = defaultdict(int)
n = 0; total_uncompressed = 0; rec_sizes = []
for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    n += 1; total_uncompressed += len(line) + 1; rec_sizes.append(len(line) + 1)
    d = json.loads(line)
    for f, (typ, multi) in FIELDS.items():
        if f not in d or d[f] is None:
            continue
        present[f] += 1; v = d[f]
        if multi:
            for el in (v if isinstance(v, list) else [v]):
                s = str(el); distinct[f].add(s); sumlen[f] += len(s); count[f] += 1
        else:
            s = v if isinstance(v, str) else json.dumps(v)
            distinct[f].add(v if isinstance(v, (str, int, float, bool)) else s)
            sumlen[f] += len(s); count[f] += 1

rec_sizes.sort()
ratio = os.path.getsize(COMPRESSED) / total_uncompressed
print("recordCount", n)
print("datasetBytesCompressed", os.path.getsize(COMPRESSED))
print("p95RecordCompressed", round(rec_sizes[int(0.95 * len(rec_sizes))] * ratio))
for f, (typ, multi) in FIELDS.items():
    print(f, "type", typ, "multi", multi, "card", len(distinct[f]),
          "avgBytes", round(sumlen[f] / count[f]) if count[f] else 0,
          "absentable", present[f] < n)
