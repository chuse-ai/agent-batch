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

/**
 * The result a job must produce: a JSON Schema (2020-12 unless `$schema` says otherwise) describing
 * a single value, not a list, and referring to nothing outside itself.
 */
export const resultContractSchema = z.object({ schema: z.record(z.string(), z.unknown()) });
export type ResultContract = z.infer<typeof resultContractSchema>;

/** What the agent is asked to do. */
export const promptSchema = textSchema.check(z.minLength(1));
