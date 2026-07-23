import { connect } from "./shard-db/client.js";

const db = connect();

function renderList(el: HTMLUListElement, records: { title: string; year: number; rating: number; director: string }[]) {
  el.innerHTML = "";
  for (const r of records) {
    const li = document.createElement("li");
    li.textContent = `${r.title} (${r.year}) — ${r.rating}/10, dir. ${r.director}`;
    el.appendChild(li);
  }
}

async function runSearch(term: string) {
  const el = document.getElementById("search-results") as HTMLUListElement;
  const { records } = await db.movies.findMany({ where: { title: { contains: term } } });
  renderList(el, records);
}

async function runFiltered() {
  const el = document.getElementById("filtered-results") as HTMLUListElement;
  const { records } = await db.movies.findMany({
    where: { year: { gte: 2015, lte: 2020 } },
    orderBy: { rating: "desc" },
    limit: 10,
  });
  renderList(el, records);
}

async function main() {
  const { count } = await db.movies.count();
  document.getElementById("total-count")!.textContent = `${count} movies in the catalog`;

  await runSearch((document.getElementById("search-input") as HTMLInputElement).value);
  await runFiltered();

  document.getElementById("search-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    void runSearch((document.getElementById("search-input") as HTMLInputElement).value);
  });
}

void main();
