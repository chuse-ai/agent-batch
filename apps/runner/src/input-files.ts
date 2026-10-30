import { createHash } from "node:crypto";
import path from "node:path";
import { zstdDecompressSync } from "node:zlib";

import { ORPCError } from "@orpc/client";
import { type ListedInputFile, problemOf } from "contract";
import { Data, Duration, Effect, type FileSystem, Schedule } from "effect";
import ensureError from "ensure-error";

import { type AgentSettings, type HubHttpClient, jobPaths } from "./agent.ts";

// A job's Input files reach its Workspace apart from the hub's answers, which only name them: the
// Runner downloads each a chunk at a time, one zstd frame per request, decompresses it, and checks
// the whole file's size and SHA-256 before moving it into `inputs/`. Each request carries a whole
// frame because Node's zstd decoder before 26.10 stops at the end of the first frame of a stream
// that holds several, and the Runner supports Node 22. So neither side holds more than a chunk.

/** A job's Input file that could not be downloaded, naming the file and why. */
export class InputFileFailed extends Data.TaggedError("InputFileFailed")<{ message: string }> {}

/** A chunk that did not arrive, and whether asking again may bring it. */
class ChunkUnavailable extends Data.TaggedError("ChunkUnavailable")<{
  reason: string;
  transient: boolean;
}> {}

/** How long one chunk is asked for while the hub cannot answer, before the download fails. */
const RETRY_DEADLINE = Duration.minutes(5);

/**
 * How one chunk is asked for again while the hub cannot answer, as when it restarts or its database
 * is unreachable: waits of about 1 s growing to 30 s, each varied so Runners do not ask together.
 */
const RETRIES = Schedule.min([
  Schedule.exponential(Duration.seconds(1)),
  Schedule.spaced(Duration.seconds(30)),
]).pipe(Schedule.jittered);

/**
 * Downloads the job `jobId`'s Input files `files` from the hub into its Workspace's `inputs/`,
 * through a temporary file each in its folder's `downloads/`. A file whose download ends in other
 * bytes than the hub listed is downloaded once more, in case it was the transfer; a second
 * mismatch, a file the hub no longer has, or a hub that does not answer for five minutes fails with
 * `InputFileFailed`.
 */
export function downloadInputFiles(
  { hubHttp, fileSystem, jobsDir }: Pick<AgentSettings, "hubHttp" | "fileSystem" | "jobsDir">,
  jobId: string,
  files: readonly ListedInputFile[],
): Effect.Effect<void, InputFileFailed> {
  const paths = jobPaths(jobsDir, jobId);
  const { downloads } = paths;
  const inputs = path.resolve(paths.workspace, "inputs");
  return Effect.gen(function* () {
    yield* fileSystem.makeDirectory(downloads, { recursive: true });
    yield* fileSystem.makeDirectory(inputs, { recursive: true });
    for (const [index, file] of files.entries()) {
      // The contract checks each path as the run reaches the Runner (`filePathSchema`): relative,
      // without `.` or `..`. Checked again here, where it becomes a place to write.
      const target = path.resolve(inputs, file.path);
      if (!target.startsWith(`${inputs}${path.sep}`)) {
        yield* Effect.fail(
          new InputFileFailed({ message: `The Input file ${file.path} is not inside \`inputs\`` }),
        );
      }
      // By position, as a path's folders do not exist in `downloads/`.
      const temporary = path.join(downloads, String(index));
      yield* downloadFile(hubHttp, fileSystem, jobId, file, temporary).pipe(
        Effect.retry({ times: 1, while: (error) => error instanceof Mismatch }),
        Effect.catchTag("Mismatch", (mismatch) =>
          Effect.fail(new InputFileFailed({ message: inputFileMessage(file, mismatch.reason) })),
        ),
      );
      yield* fileSystem.makeDirectory(path.dirname(target), { recursive: true });
      yield* fileSystem.rename(temporary, target);
    }
    yield* fileSystem.remove(downloads, { recursive: true });
  }).pipe(
    Effect.catchTag("PlatformError", (error) =>
      Effect.fail(
        new InputFileFailed({ message: `Writing the Input files failed: ${error.message}` }),
      ),
    ),
  );
}

/** Why downloading `file` failed, naming it. */
function inputFileMessage(file: ListedInputFile, reason: string): string {
  return `Downloading the Input file ${file.path} failed: ${reason}`;
}

/** Bytes that were not those the hub listed, as the hub's checks or the transfer changed them. */
class Mismatch extends Data.TaggedError("Mismatch")<{ reason: string }> {}

/** Downloads `file` into `temporary`, chunk by chunk, checking its size and SHA-256. */
function downloadFile(
  hubHttp: HubHttpClient,
  fileSystem: FileSystem.FileSystem,
  jobId: string,
  file: ListedInputFile,
  temporary: string,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const out = yield* fileSystem.open(temporary, { flag: "w" });
      const hash = createHash("sha256");
      let written = 0;
      for (let index = 0; written < file.originalSizeBytes; index++) {
        const frame = yield* fetchChunk(hubHttp, jobId, file.path, index).pipe(
          Effect.catchTag("ChunkUnavailable", (unavailable) =>
            Effect.fail(
              new InputFileFailed({ message: inputFileMessage(file, unavailable.reason) }),
            ),
          ),
        );
        // A chunk holds at most the listed `chunkBytes`, and the file no more than it was listed
        // with: decompressing stops there rather than writing more.
        const expected = Math.min(file.chunkBytes, file.originalSizeBytes - written);
        const bytes = yield* Effect.try({
          try: () => zstdDecompressSync(frame, { maxOutputLength: expected }),
          catch: (error) =>
            new Mismatch({
              reason: `chunk ${index} did not decompress: ${ensureError(error).message}`,
            }),
        });
        // A chunk short of its share means the file is shorter than listed; asking for the next
        // chunk would only report it missing.
        if (bytes.length < expected) {
          yield* Effect.fail(
            new Mismatch({ reason: `chunk ${index} holds fewer bytes than the file's size needs` }),
          );
        }
        hash.update(bytes);
        written += bytes.length;
        yield* out.writeAll(bytes);
      }
      const digest = `sha256:${hash.digest("hex")}`;
      if (digest !== file.digest) {
        yield* Effect.fail(
          new Mismatch({ reason: `its digest is ${digest}, not ${file.digest} as listed` }),
        );
      }
    }),
  );
}

/** How long one request for a chunk may take, so a hub that stops answering is asked again. */
const CHUNK_TIMEOUT = Duration.seconds(60);

/** Chunk `index` of the job's Input file at `path`, as one zstd frame, asked for again while it may come. */
function fetchChunk(hubHttp: HubHttpClient, jobId: string, filePath: string, index: number) {
  return Effect.tryPromise({
    try: async (signal) =>
      (await hubHttp.getInputFileChunk({ id: jobId, path: filePath, index }, { signal })).bytes(),
    catch: (cause) => {
      // The status of an answer from the hub's side, a proxy's included, from the Problem Details
      // the contract's link keeps; an error oRPC raises itself, such as an answer the contract does
      // not allow, has none. Any other failure is the hub not reached, or its answer broken off.
      const status = cause instanceof ORPCError ? (problemOf(cause)?.status ?? 0) : 503;
      if (status === 404) {
        return new ChunkUnavailable({
          reason: `chunk ${index} is not on the hub, or the job is no longer placed on this Runner`,
          transient: false,
        });
      }
      return new ChunkUnavailable({
        reason: `chunk ${index}: ${ensureError(cause).message}`,
        transient: status === 429 || status >= 500,
      });
    },
  }).pipe(
    // Interrupting the request aborts it through its signal.
    Effect.timeoutOrElse({
      duration: CHUNK_TIMEOUT,
      orElse: () =>
        Effect.fail(
          new ChunkUnavailable({
            reason: `chunk ${index} did not arrive within ${Duration.format(CHUNK_TIMEOUT)}`,
            transient: true,
          }),
        ),
    }),
    Effect.retry({ schedule: RETRIES, while: (unavailable) => unavailable.transient }),
    // The schedule's limit is checked between attempts, so the last attempt could run past it.
    Effect.timeoutOrElse({
      duration: RETRY_DEADLINE,
      orElse: () =>
        Effect.fail(
          new ChunkUnavailable({
            reason: `chunk ${index} did not arrive within ${Duration.format(RETRY_DEADLINE)}`,
            transient: false,
          }),
        ),
    }),
  );
}
