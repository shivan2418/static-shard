import readline from "node:readline";
import path from "node:path";
import { init, resolveInitConfig, sampleRecords, type InitOptions, type InitResult } from "./init.js";
import { readInputRecords } from "./input.js";
import {
  applyKey,
  buildWizardData,
  createInitialState,
  deriveWizardChoices,
  estimateForState,
  renderFrame,
  type WizardChoices,
  type WizardData,
  type WizardKey,
  type WizardState,
} from "./wizard.js";
import type { InputFormat } from "./types.js";

const CLEAR_SCREEN = "\x1b[2J\x1b[H";

type TTYStdin = NodeJS.ReadStream & { setRawMode?: (mode: boolean) => void; isRaw?: boolean };

export interface InteractiveInitOptions
  extends Pick<
    InitOptions,
    "cwd" | "configPath" | "inputPath" | "format" | "delimiter" | "records" | "collection" | "fullScan" | "sampleSize" | "output" | "clientOut" | "basePath" | "indexChunkBytes"
  > {
  stdin?: NodeJS.ReadStream;
  stdout?: NodeJS.WriteStream;
}

/**
 * ADR-0006 §4's no-TTY fallback: rather than block forever on keypresses that will never arrive
 * (e.g. `init` run in CI or piped), the wizard fails loud immediately with the same "pass --yes"
 * guidance `init()`'s own non-interactive guard gives.
 */
export function ensureInteractiveTTY(stdin: Pick<NodeJS.ReadStream, "isTTY">): void {
  if (!stdin.isTTY) {
    throw new Error(
      "static-shard: init needs an interactive terminal for the wizard — pass --yes plus flags to run non-interactively",
    );
  }
}

function keyFromInput(str: string | undefined, key: readline.Key): WizardKey | undefined {
  if (key?.ctrl && key.name === "c") return { type: "cancel" };
  switch (key?.name) {
    case "up":
      return { type: "up" };
    case "down":
      return { type: "down" };
    case "left":
      return { type: "left" };
    case "right":
      return { type: "right" };
    case "return":
      return { type: "enter" };
    case "backspace":
      return { type: "backspace" };
    case "space":
      return { type: "space" };
  }
  if (str && str.length === 1 && str >= " " && str !== "\x7f") return { type: "char", value: str };
  return undefined;
}

/**
 * Translates the wizard's chosen knobs into the same `InitOptions` shape `init --yes` + flags would
 * receive — the single place that does this, used both for the review step's live preview
 * (`resolveInitConfig`, no write) and for the actual persist (`init`, writes). One call site means
 * there's no second translation that could drift from it.
 */
function toInitOptions(opts: InteractiveInitOptions, choices: WizardChoices): InitOptions {
  return {
    cwd: opts.cwd,
    configPath: opts.configPath,
    yes: true,
    reinfer: true,
    fullScan: opts.fullScan,
    sampleSize: opts.sampleSize,
    collection: opts.collection,
    inputPath: opts.inputPath,
    format: opts.format,
    delimiter: opts.delimiter,
    records: opts.records,
    sortField: choices.sortField,
    indexedFields: choices.indexedFields,
    endsWithFields: choices.endsWithFields,
    containsFields: choices.containsFields,
    output: opts.output,
    clientOut: opts.clientOut,
    basePath: opts.basePath,
    shardBytes: choices.shardBytes,
    indexChunkBytes: opts.indexChunkBytes,
  };
}

/**
 * The interactive `init` wizard (T12/ADR-0006): a thin terminal driver over `wizard.ts`'s pure state
 * machine. On persist it calls the exact same `init()` core the non-interactive `--yes` path uses —
 * flag-equivalence (ADR-0006 §1) isn't asserted after the fact, it's structural: there is no second
 * config-writing code path for the wizard to drift from.
 */
export function runInteractiveInit(opts: InteractiveInitOptions): Promise<InitResult> {
  const stdin = (opts.stdin ?? process.stdin) as TTYStdin;
  const stdout = opts.stdout ?? process.stdout;
  ensureInteractiveTTY(stdin);

  const inputPath = opts.inputPath;
  if (!inputPath) {
    throw new Error("static-shard: init needs an input path/glob — pass it as the positional argument");
  }
  const format: InputFormat = opts.format ?? "ndjson";
  const delimiter = opts.delimiter ?? (format === "tsv" ? "\t" : ",");

  const allRecords = readInputRecords(path.resolve(opts.cwd, inputPath), {
    format,
    delimiter,
    recordsPath: opts.records,
    fields: {},
  });
  const sample = sampleRecords(allRecords, opts);
  const data: WizardData = buildWizardData(sample);

  let state: WizardState = createInitialState(data);

  return new Promise((resolve, reject) => {
    const wasRaw = stdin.isRaw;

    function cleanup(): void {
      stdin.off("keypress", onKeypress);
      stdin.setRawMode?.(wasRaw ?? false);
      stdin.pause();
    }

    function render(): void {
      const estimate = estimateForState(data, state);
      let configPreview: string | undefined;
      if (state.stage === 5 && state.reviewJsonExpanded) {
        try {
          const initOptions = toInitOptions(opts, deriveWizardChoices(state));
          configPreview = JSON.stringify(resolveInitConfig(initOptions).config, null, 2);
        } catch (err) {
          configPreview = `(preview unavailable: ${err instanceof Error ? err.message : String(err)})`;
        }
      }
      stdout.write(CLEAR_SCREEN + renderFrame(data, state, estimate, configPreview));
    }

    function onKeypress(str: string | undefined, key: readline.Key): void {
      const wizardKey = keyFromInput(str, key);
      if (!wizardKey) return;
      state = applyKey(data, state, wizardKey);

      if (state.quit) {
        cleanup();
        reject(new Error("static-shard: init wizard cancelled"));
        return;
      }
      if (state.persisted) {
        cleanup();
        try {
          resolve(init(toInitOptions(opts, deriveWizardChoices(state))));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
        return;
      }
      render();
    }

    readline.emitKeypressEvents(stdin);
    stdin.setRawMode?.(true);
    stdin.on("keypress", onKeypress);
    render();
  });
}
