import isValidFilename from "valid-filename";
import * as z from "zod/mini";

// What a job is made of, as a caller submits it and as a Runner receives it: the same schemas on
// both sides, so neither can drift from the other.

/**
 * A job's ID, as the API and the Runner protocol carry it: a UUID, so a Runner that names a job's
 * files by it stays inside its jobs directory.
 */
export const jobIdSchema = z.uuid();

/** A job as a call names it. */
export const jobRefSchema = z.object({ id: jobIdSchema.check(z.describe("The job's ID.")) });

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

/** Linux's `PATH_MAX`, in bytes, the most a path under `inputs/` may take. */
const MAX_PATH_BYTES = 4096;

const utf8Length = (text: string) => new TextEncoder().encode(text).length;

/**
 * An Input file's path in the job's `inputs` folder: file names, valid wherever a Runner writes
 * them, separated by `/`, such as `invoice.pdf` or `claims/42/photo.jpg`. Relative, so neither a
 * leading `/`, an empty name, `.` nor `..` (which `valid-filename` refuses) can leave the folder.
 */
export const filePathSchema = textSchema.check(
  z.refine(
    (path) =>
      utf8Length(path) <= MAX_PATH_BYTES &&
      path.split("/").every((name) => isValidFilename(name) && utf8Length(name) <= MAX_NAME_BYTES),
    `Must be a relative path of file names valid on Linux, macOS and Windows, separated by \`/\`, each of at most ${MAX_NAME_BYTES} bytes, of at most ${MAX_PATH_BYTES} bytes in all`,
  ),
);

/** Each path that `path` lies in, nearest last: `a` and `a/b` for `a/b/c`. */
function foldersOf(path: string): string[] {
  const names = path.split("/");
  return names.slice(1).map((_, index) => names.slice(0, index + 1).join("/"));
}

/** The most bytes an Input file may hold, as one upload's request carries it whole. */
export const MAX_FILE_BYTES = 100 * 1024 * 1024;

/**
 * Stored content's identity: `sha256:` and the SHA-256 of its bytes as lowercase hex, the algorithm
 * written in the value, as OCI's digests and Git LFS's pointers write it.
 */
export const digestSchema = z
  .string()
  .check(z.regex(/^sha256:[0-9a-f]{64}$/, "Must be `sha256:` and 64 lowercase hexadecimal digits"));

/** How many bytes a file holds: at most `MAX_FILE_BYTES`. */
export const fileSizeBytesSchema = z.int().check(z.nonnegative(), z.maximum(MAX_FILE_BYTES));

/** Stored content as a caller names it: its digest and its size in bytes. */
export const blobRefSchema = z.object({
  digest: digestSchema.check(z.describe("The content's digest, such as `sha256:9f86d0…`.")),
  sizeBytes: fileSizeBytesSchema.check(z.describe("The content's size in bytes.")),
});

/**
 * An Input file: its path in the job's `inputs` folder and the stored content it holds, named by
 * digest and size, as uploaded before the job.
 */
export const inputFileSchema = z.extend(blobRefSchema, { path: filePathSchema });

/**
 * Files written into the job's `inputs` folder before its agent starts, as sandbox APIs take them: a
 * list of paths and contents, so a path, folders included, is a value rather than a key. No two may
 * collide, as the same path or as a file and a folder.
 */
export const inputFilesSchema = z.array(inputFileSchema).check(
  z.superRefine((files, context) => {
    const first = new Map<string, number>();
    files.forEach(({ path }, index) => {
      if (!first.has(path)) first.set(path, index);
    });
    files.forEach(({ path }, index) => {
      const same = first.get(path);
      const folder = foldersOf(path).find((name) => first.has(name));
      const message =
        same !== undefined && same < index
          ? `Must be unique, as files[${same}] has it`
          : folder !== undefined
            ? `Must not be in a folder that files[${first.get(folder)}] is as a file`
            : undefined;
      if (message !== undefined) {
        context.addIssue({ code: "custom", message, path: [index, "path"], input: path });
      }
    });
  }),
);

/**
 * An Input file as the hub lists it to a Runner: its path, the digest and size of its original
 * bytes, which the Runner checks what it downloads against, and how many original bytes each of its
 * chunks holds but the last, which holds the rest. The hub compresses each chunk as one zstd frame,
 * and the Runner downloads a file a chunk at a time, so chunk `n` starts at byte `n * chunkBytes`.
 * The hub stores each file with the chunk size it chose then, so it can choose another for new files.
 */
export const listedInputFileSchema = z.object({
  path: filePathSchema,
  digest: digestSchema,
  originalSizeBytes: z.int().check(z.nonnegative()),
  chunkBytes: z.int32().check(z.positive()),
});
export type ListedInputFile = z.infer<typeof listedInputFileSchema>;

/**
 * The output a job must produce: a JSON Schema (2020-12 unless `$schema` says otherwise) describing
 * a single value, not a list, and referring to nothing outside itself.
 */
export const outputContractSchema = z.object({ schema: z.record(z.string(), z.unknown()) });
export type OutputContract = z.infer<typeof outputContractSchema>;

/** What one job gives its Batch's agent to work on: the agent's first input, after its instructions. */
export const inputSchema = textSchema.check(z.minLength(1));

/** What an agent is told beside the Runner's own instructions, before any job's input. */
export const instructionsSchema = textSchema.check(z.minLength(1));

/**
 * A caller's own ID for a job or a Batch: adding or creating it again with the same content
 * returns the one already accepted, and with other content is refused.
 */
export const customIdSchema = textSchema.check(z.minLength(1), z.maxLength(255));

/**
 * A label's key, in Kubernetes's syntax, so it reads as one parameter of a list's
 * `labels[key]=value` filter: a name of at most 63 characters, beginning and ending with a letter or
 * digit, with `-`, `_` and `.` between, optionally behind a DNS subdomain prefix and `/`, such as
 * `customer` or `example.com/team`.
 */
export const labelKeySchema = z
  .string()
  .check(
    z.regex(
      /^(?:(?=.{1,253}\/)[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*\/)?[A-Za-z0-9](?:[-_.A-Za-z0-9]{0,61}[A-Za-z0-9])?$/,
      "Must be a name of at most 63 letters, digits, `-`, `_` and `.`, beginning and ending with a letter or digit, optionally behind a DNS subdomain and `/`",
    ),
  );

/**
 * A label's value: any Unicode text of at most 256 characters, which Zod counts as code points, kept
 * exactly as given, without trimming, case folding or normalization.
 */
export const labelValueSchema = textSchema.check(z.maxLength(256));

/** The most labels a job carries, its Batch's included. */
export const MAX_LABELS = 64;

/**
 * A job's labels, by key: what it is for, to find it, group it and attribute its cost by. The limit
 * is a refinement, which JSON Schema cannot carry, so `maxProperties` states it there.
 */
export const labelsSchema = z.record(labelKeySchema, labelValueSchema).check(
  z.refine(
    (labels) => Object.keys(labels).length <= MAX_LABELS,
    `Must have at most ${MAX_LABELS} labels`,
  ),
  z.meta({ maxProperties: MAX_LABELS }),
);
export type Labels = z.infer<typeof labelsSchema>;
