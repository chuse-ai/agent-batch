import isValidFilename from "valid-filename";
import * as z from "zod/mini";

// What a job is made of, as a caller submits it and as a Runner receives it: the same schemas on
// both sides, so neither can drift from the other.

/**
 * A job's ID, as the API and the Runner protocol carry it: a UUID, so a Runner that names a job's
 * files by it stays inside its jobs directory.
 */
export const jobIdSchema = z.uuid();

/**
 * Text as a job keeps it. JSON can carry a lone UTF-16 surrogate, which is not Unicode text and which
 * neither UTF-8 storage nor canonical JSON accepts, and NUL, which Postgres text cannot hold.
 */
export const textSchema = z
  .string()
  .check(
    z.refine(
      (text) => text.isWellFormed() && !text.includes("\0"),
      "Must be Unicode text without NUL",
    ),
  );

const recursiveJsonTextSchema: z.ZodMiniType<z.infer<ReturnType<typeof z.json>>> = z.lazy(() =>
  z.union([
    textSchema,
    z.number(),
    z.boolean(),
    z.null(),
    z.array(recursiveJsonTextSchema),
    // Keys are checked as the record's own refinement: a key the record's key schema refused would
    // fail every option of the union, whose error then names neither the key nor the rule.
    z.record(z.string(), recursiveJsonTextSchema).check(
      z.superRefine((record, context) => {
        for (const key of Object.keys(record)) {
          const parsed = z.safeParse(textSchema, key);
          for (const issue of parsed.success ? [] : parsed.error.issues) {
            context.addIssue({ ...issue, path: [key] });
          }
        }
      }),
    ),
  ]),
);

// Zod recurses and can throw past roughly 1,000 levels on Node 26 and workerd, even in safeParse.
// None of our dependencies caps depth on a parsed value, so probe before entering the schema.
// Allow 64 nested containers, counting the root as one: .NET's default, below serde_json's 128.
// The probe stops at the first container beyond the limit, so its own recursion stays bounded.
function withinJsonDepth(value: unknown, remaining = 64): boolean {
  return (
    value === null ||
    typeof value !== "object" ||
    (remaining > 0 && Object.values(value).every((child) => withinJsonDepth(child, remaining - 1)))
  );
}

/**
 * A JSON value nested at most 64 containers deep, whose strings and keys are text a job keeps
 * (`textSchema`), as Postgres's `jsonb` stores it: `jsonb` refuses NUL and lone surrogates too.
 */
export const jsonTextSchema = z.pipe(
  z.unknown().check(z.refine(withinJsonDepth, "JSON must not nest more than 64 containers")),
  recursiveJsonTextSchema,
);

/**
 * How long each run of a job on a Runner may work, in whole seconds: above zero, and no more than a
 * Postgres `integer` holds, where the hub keeps it.
 */
export const runTimeoutSecondsSchema = z.int32().check(z.positive());

/** The most a job's model calls may cost over its whole life, in US dollars: above zero. */
export const modelBudgetUsdSchema = z.number().check(z.positive());

/** File systems limit a file name to 255 bytes of UTF-8; `valid-filename` counts UTF-16 units. */
const MAX_NAME_BYTES = 255;

/** An Input file's name: one file name, without folders, valid wherever a Runner writes it. */
export const fileNameSchema = textSchema.check(
  z.refine(isValidFilename),
  z.refine((name) => new TextEncoder().encode(name).length <= MAX_NAME_BYTES),
);

/** An Input file's content, given inline as UTF-8 text. */
export const inputFileSchema = z.object({ content: textSchema });

/**
 * Files written into the job's Workspace before its agent starts, by file name, as GitHub's Gists
 * take them: the agent finds them all in one folder, and no two can share a name. Zod reports a bad
 * key without the key's own messages, so the record says what a name must be.
 */
export const inputFilesSchema = z.record(fileNameSchema, inputFileSchema, {
  error: (issue) =>
    issue.code === "invalid_key"
      ? `Must be a file name that is valid on Linux, macOS and Windows, of at most ${MAX_NAME_BYTES} bytes`
      : undefined,
});

/** A SHA-256 digest as lowercase hex, as the hub names an Input file's blob. */
export const sha256Schema = z.hash("sha256").check(z.lowercase());

/**
 * An Input file as the hub lists it to a Runner: its name, the SHA-256 and size of its original
 * bytes, which the Runner checks what it downloads against, and how many original bytes each of its
 * chunks holds but the last, which holds the rest. The hub compresses each chunk as one zstd frame,
 * and the Runner downloads a file a chunk at a time, so chunk `n` starts at byte `n * chunkBytes`.
 * The hub stores each file with the chunk size it chose then, so it can choose another for new files.
 */
export const listedInputFileSchema = z.object({
  name: fileNameSchema,
  sha256: sha256Schema,
  originalSizeBytes: z.int().check(z.nonnegative()),
  chunkBytes: z.int32().check(z.positive()),
});
export type ListedInputFile = z.infer<typeof listedInputFileSchema>;

/**
 * The result a job must produce: a JSON Schema (2020-12 unless `$schema` says otherwise) describing
 * a single value, not a list, and referring to nothing outside itself.
 */
export const resultContractSchema = z.object({ schema: z.record(z.string(), z.unknown()) });
export type ResultContract = z.infer<typeof resultContractSchema>;

/** What the agent is asked to do. */
export const promptSchema = textSchema.check(z.minLength(1));
