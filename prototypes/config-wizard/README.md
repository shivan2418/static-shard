# PROTOTYPE — `init` config wizard (wayfinder T7, issue #10)

**Throwaway.** Answers a UI/UX question, not a logic one: *how should the `static-shard init` wizard present the three knobs, their live consequence estimates, the per-field operator toggles (with the loud `contains` warning), and the skew/oversize/low-card warnings — and what flow ties them together?*

The ADRs already locked the **content** (ADR-0002 the three knobs + sharding, ADR-0003 §7 the operator toggles, §11 the cost formulas). What was open is the **shape and feel**. Explored in two passes.

## Pass 1 — which *shape*? (`tui.ts`, zero-dep)

A raw-ANSI explorer with **three radically different shapes on one switcher** (`[Tab]`/`[1/2/3]`): **A — Linear wizard** (one decision per screen), **B — Live cockpit** (everything on one dense screen), **C — Review & override** (tool decides, you veto). **Shape A won.**

```
node prototypes/config-wizard/tui.ts        # Node >= 22 strips the types natively
```

`tui.ts` also doubles as the **zero-dep rendering proof**: it re-renders the whole frame on every keystroke, so live estimates work with no dependencies (inline, above the controls).

## Pass 2 — the feel, and the dependency call (`wizard-ink.tsx`)

Shape A rebuilt in **Ink** (React for the terminal) to judge the real look and settle the interaction model. This file is the **validated UX reference**:

- `←/→` move between stages · `↑/↓` move within a step · `space` choose/toggle
- **flat operator checklist** — one row per field × operator (`ends with` / `contains`), toggled with `space` like any other list (replaced an unintuitive focused-card + letter-key design)
- **file size** as a `↑/↓` list, each option showing its file count
- **live "Consequences" panel** beside the controls (Ink's flexbox), incl. an *Extra search indexes* line that reacts as you toggle operators
- **review** leads with the plain summary; the config file is collapsed (`v` to preview)

```
cd prototypes/config-wizard
pnpm install        # first time only (ink, react, tsx — prototype-only, gitignored)
pnpm ink
```

### Decision: validated in Ink, shipped dep-light

Ink looked best, **but the real `static-shard-cli` will not take the React dependency.** Ink drags in `react` + `react-reconciler` + `yoga-layout`; for a tool whose whole pitch is minimalism that footprint isn't worth it — even though it's build-time-only and the runtime `static-shard` package stays zero-dep regardless (ADR-0005).

So: **Ink = the UX/flow reference; the CLI implements it hand-rolled (zero deps, like `tui.ts`) or with one tiny prompt lib.** The only thing lost going dep-light is the flexbox side *panel* — live estimates move inline (which `tui.ts` already does). Everything else (the flow, the flat checklist, the file-size list, the collapsed review) ports directly.

## The data is real

`profile.ts` is the actual profile of a **116,138-record, 72.5 MB Scryfall MTG-card export** (`default-cards-*.jsonl.gz`), measured the way `init` would (`profile-scryfall.py` regenerates it). **Curated to 12 fields for a legible demo** — the real record has 63 top-level keys → **~91 scalar-leaf paths** once nested objects (`legalities.*`, `prices.*`, …) are expanded, plus 9 arrays. That scale is a real design requirement carried into the T7 spec (the field-list + operator steps need a scrollable/filterable list and a *small recommended* default index set, opt-in — not the prototype's opt-out-of-everything).

## The estimator is the reusable core

`estimator.ts` is a faithful implementation of the **ADR-0003 §11** cost formulas (folding in ADR-0002 §5). No I/O, no terminal code — the bit worth lifting into the real build. That every shell (`tui`, `ink`) sits on it unchanged is the proof it's portable.

## Files

| File | Role |
|---|---|
| `profile.ts` | the real detected dataset, curated to 12 fields (portable) |
| `estimator.ts` | ADR-0003 §11 cost formulas — **the reusable core** (portable, pure) |
| `wizard-shared.ts` | Shape-A wizard logic over the estimator (renderer-agnostic) |
| `tui.ts` | pass-1 explorer: 3 shapes + switcher; also the **zero-dep rendering proof** |
| `wizard-ink.tsx` | pass-2: Shape A in Ink — the **validated UX reference** (not the shipping lib) |
| `profile-scryfall.py` | regenerates `profile.ts`'s numbers from the raw `.jsonl.gz` |
