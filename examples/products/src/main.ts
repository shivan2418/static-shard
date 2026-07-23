import { connect } from "./shard-db/client.js";

const db = connect();

async function runLookup(sku: string) {
  const el = document.getElementById("lookup-result")!;
  const record = await db.products.get(sku);
  el.textContent = record
    ? `${record.name} — $${record.price} (${record.category}), ${record.inStock ? "in stock" : "out of stock"}`
    : `no product with SKU "${sku}"`;
}

async function runDiscounted() {
  const el = document.getElementById("discounted-results") as HTMLUListElement;
  const { records } = await db.products.findMany({
    where: { inStock: { equals: true }, discountPct: { exists: true } },
    orderBy: { price: "asc" },
    limit: 10,
  });
  el.innerHTML = "";
  for (const r of records) {
    const li = document.createElement("li");
    li.textContent = `${r.name} — $${r.price} (${r.discountPct}% off), ${r.category}`;
    el.appendChild(li);
  }
}

async function main() {
  const { count } = await db.products.count();
  document.getElementById("total-count")!.textContent = `${count} products in the catalog`;

  await runLookup((document.getElementById("lookup-input") as HTMLInputElement).value);
  await runDiscounted();

  document.getElementById("lookup-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    void runLookup((document.getElementById("lookup-input") as HTMLInputElement).value);
  });
}

void main();
