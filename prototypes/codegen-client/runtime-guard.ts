// ============================================================================
// PROTOTYPE — THROWAWAY. The rider rule (ADR-0003 §7) is enforced at COMPILE
// time (see consumer.ts) — this file demonstrates the DEFENSE-IN-DEPTH runtime
// guard that ALSO catches it, for untyped JS callers and dynamically-built
// where objects the compiler never sees. The two rider-only calls below are
// marked expect-error (the type layer rejects them too); tsx runs them anyway
// (types stripped), proving the runtime guard fires.
// Run:  pnpm --package=tsx dlx tsx runtime-guard.ts
// Exit 0 + "ALL RUNTIME GUARDS OK" = verdict.
// ============================================================================
import { connect } from "./generated/client";

const db = connect({ basePath: "/data", maxResults: 500 });

async function expectThrow(label: string, fn: () => Promise<unknown>) {
  try {
    await fn();
    throw new Error(`FAIL — expected throw but succeeded: ${label}`);
  } catch (e) {
    if (e instanceof Error && e.message.startsWith("FAIL")) throw e;
    console.log(`  ok (threw): ${label}`);
  }
}
async function expectOk(label: string, fn: () => Promise<unknown>) {
  await fn();
  console.log(`  ok (passed): ${label}`);
}

async function main() {
  // Sole `not` → throws. Types reject it too (@ts-expect-error); here it stands
  // in for an untyped JS caller, proving the runtime guard also fires.
  await expectThrow("inPrint.not alone", () =>
    // @ts-expect-error rider-only is a compile error; simulating a JS caller
    db.movies.findMany({ where: { inPrint: { not: true } } }),
  );

  // `not` alongside a pruning constraint → allowed.
  await expectOk("year.gte + inPrint.not", () =>
    db.movies.findMany({ where: { year: { gte: 2000 }, inPrint: { not: false } } }),
  );
  // contains / endsWith PRUNE → valid as a sole constraint (NOT riders).
  await expectOk("title.contains alone", () =>
    db.movies.findMany({ where: { title: { contains: "Matrix" } } }),
  );
  // Empty where (findMany-all) → allowed.
  await expectOk("empty where", () => db.movies.findMany());

  // maxResults guardrail: explicit limit above the client ceiling → throws.
  await expectThrow("limit 5000 > maxResults 500", () =>
    db.movies.findMany({ where: { year: { gte: 2000 } }, limit: 5000 }),
  );
  // limit within the ceiling → allowed.
  await expectOk("limit 100 ≤ maxResults 500", () =>
    db.movies.findMany({ where: { year: { gte: 2000 } }, limit: 100 }),
  );

  console.log("ALL RUNTIME GUARDS OK");
}

main().catch((e) => {
  console.error(e);
  throw e; // non-zero exit via unhandled rejection
});
