import type {
  AuthOperationOptions,
  Credential,
  CredentialInfo,
  CredentialStore,
} from "@earendil-works/pi-ai";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import { Effect, FileSystem, Predicate, Schedule } from "effect";
import lockfile from "proper-lockfile";
import writeFileAtomic from "write-file-atomic";

// Pi keeps a sign-in's tokens in a `CredentialStore` it is given and refreshes them itself, inside
// `modify`, saving the new tokens before `modify` returns. OpenAI's refresh tokens work once, so two
// refreshes of the same token, by the Runner and by a `login` beside it or by two of the Runner's
// own model calls, must not overlap: each change holds a lock on the file across Pi's callback, as
// Pi's coding agent's own `auth.json` store does. That store is not exported; the coding agent's
// `ModelRuntime`, which Pi offers in its place, would load the whole coding agent into every Runner
// (about 50 MB more memory) and resolve models its own way.

type Credentials = Record<string, Credential>;

/**
 * How long a change waits for another holder of the lock, such as a refresh, which Pi gives up on
 * after 15 seconds, before failing.
 */
const lockWait = Schedule.spaced("100 millis").pipe(
  Schedule.jittered,
  Schedule.upTo({ duration: "30 seconds" }),
);

/**
 * Model credentials, such as a ChatGPT sign-in, in the JSON file `file`, one per provider as Pi
 * names it, readable only by the operating-system user running the Runner. A change replaces the
 * file whole, so a read needs no lock: it sees the file before the change or after it.
 */
export function fileCredentialStore(file: string): CredentialStore {
  const readAll = Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readFileString(file).pipe(
      Effect.map((saved): Credentials => JSON.parse(saved)),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed<Credentials>({})),
    );
  });

  /**
   * Holds the file's lock until the scope closes. A lock taken as stale from a holder that stopped
   * renewing it, after `stale`, sets `lost`. The lock is a directory beside the file, so it holds
   * while the file is replaced.
   */
  const lock = (onLost: (error: Error) => void) =>
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          lockfile.lock(file, {
            realpath: false,
            // Retried by `lockWait` instead, so that `signal` can end the wait.
            retries: 0,
            // Pi's coding agent's store: well above Pi's 15-second refresh, so a busy event loop
            // that renews the lock late does not let another process take it mid-refresh.
            stale: 30_000,
            onCompromised: onLost,
          }),
        catch: (error) => error,
      }),
      // Releasing a lock that was lost fails, and changes nothing.
      (release) => Effect.promise(() => release()).pipe(Effect.ignore),
    );

  /**
   * Runs `change` on the stored credentials while holding the file's lock, and saves what it
   * returns, if anything. `signal` cancels only the wait for the lock: once a refresh has started,
   * its new tokens must be saved, as the old ones no longer work. Pi's errors pass through as
   * they are.
   */
  const changeLocked = <A>(
    change: (all: Credentials) => Promise<{ result: A; next?: Credentials }>,
    options: AuthOperationOptions | undefined,
  ): Promise<A> => {
    let lost: Error | undefined;
    return Effect.runPromise(
      Effect.gen(function* () {
        yield* lock((error) => {
          lost = error;
        }).pipe(
          Effect.retry({
            schedule: lockWait,
            while: (error) => Predicate.hasProperty(error, "code") && error.code === "ELOCKED",
          }),
        );
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const all = yield* readAll;
            const { result, next } = yield* Effect.tryPromise({
              try: () => change(all),
              catch: (error) => error,
            });
            if (next !== undefined) {
              // Another process took the lock as stale while this one held it: saving now could
              // overwrite the newer tokens it saved.
              if (lost) return yield* Effect.fail(lost);
              yield* Effect.tryPromise({
                try: () => writeFileAtomic(file, JSON.stringify(next, null, 2), { mode: 0o600 }),
                catch: (error) => error,
              });
            }
            return result;
          }),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
      { signal: options?.signal },
    );
  };

  const readAllNow = () => Effect.runPromise(readAll.pipe(Effect.provide(NodeFileSystem.layer)));

  return {
    read: async (providerId) => (await readAllNow())[providerId],
    list: async (): Promise<readonly CredentialInfo[]> =>
      Object.entries(await readAllNow()).map(([providerId, credential]) => ({
        providerId,
        type: credential.type,
      })),
    modify: (providerId, fn, options) =>
      changeLocked(async (all) => {
        const next = await fn(all[providerId]);
        return next === undefined
          ? { result: all[providerId] }
          : { result: next, next: { ...all, [providerId]: next } };
      }, options),
    delete: (providerId, options) =>
      changeLocked(async (all) => {
        const { [providerId]: _removed, ...next } = all;
        return { result: undefined, next };
      }, options),
  };
}
