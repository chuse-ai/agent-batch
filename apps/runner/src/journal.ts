import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
  type Conversation,
  type ConversationId,
  type ConversationRecord,
  defineEntry,
  type Entry,
  type EntryId,
  type EntryRecord,
  type Harness,
  LiveDoc,
  type Session,
  type Tx,
} from "@earendil-works/pi-durable";
import {
  type ConversationActivity,
  FAILURE_MESSAGE_MAX_LENGTH,
  isoDate,
  JOB_CANCELLED,
  JOB_FAILED,
  JOB_PAUSED,
  JOB_RECOVERED,
  JOB_RESUMED,
  JOB_STARTED,
  JOB_SUCCEEDED,
  type JobActivity,
  type JobEnding,
  type JobEvent,
  type JobFailure,
  type JobOpening,
  type JobPause,
  type JobSuccess,
} from "contract";
import { Array, Order } from "effect";

import runnerPackage from "../package.json" with { type: "json" };

// What the Runner records in a job's journal, and how it reads the journal's events for the hub.
// The journal's entries and conversation records become the job's events; our own entries
// record the job's start, its recovery after a Runner restart, each pause and resume, and its end,
// one kind per outcome, and the hub reads the job's lifecycle from them. Their kinds and data
// are the contract's: kinds name their subject in the past tense under `agent-batch.`, and each
// carries its time as `timestamp`, Unix milliseconds by the Runner's clock, as pi-ai's messages do.

/**
 * The Runner's version, which each entry written on opening a journal carries: each release fixes
 * pi's exact version, so it identifies the pi that wrote what follows.
 */
export const RUNNER_VERSION: string = runnerPackage.version;

/**
 * Written once, in the commit that creates the job's conversation, when its agent first starts: the
 * opening of the job's first run.
 */
export const JobStarted = defineEntry<JobOpening>(JOB_STARTED);

/** Written each time a restarted Runner reopens the job and it carries on from its journal. */
export const JobRecovered = defineEntry<JobOpening>(JOB_RECOVERED);

/**
 * Written once, as its opening, by a run that carries on a job its journal already holds: a
 * job the hub resumed after a pause, a failure or a cancel.
 */
export const JobResumed = defineEntry<JobOpening>(JOB_RESUMED);

/**
 * Written once per run, as its closing, when the hub paused the job and the Runner
 * stopped it.
 */
export const JobPaused = defineEntry<JobPause>(JOB_PAUSED);

/** Written once per run, as its closing, when the job's agent submitted an accepted output. */
export const JobSucceeded = defineEntry<JobSuccess>(JOB_SUCCEEDED);

/**
 * Written once per run, as its closing, when the job's agent stopped without an output, a limit
 * stopped it or its run failed, with why.
 */
export const JobFailed = defineEntry<JobFailure>(JOB_FAILED);

/**
 * Written once per run, as its closing, when the hub cancelled the job and the Runner
 * stopped it.
 */
export const JobCancelled = defineEntry<JobEnding>(JOB_CANCELLED);

/**
 * The request IDs a run writes its opening and its closing with, and by which a reopened
 * journal finds them. pi keeps one submission per request ID (`admitSubmission`), so each is written
 * once per run, also when a restarted Runner finds it; a job's next run, as after a
 * resume, opens and closes again.
 */
export function openingRequest(runId: string): string {
  return `agent-batch.job.opening:${runId}`;
}
export function closingRequest(runId: string): string {
  return `agent-batch.job.closing:${runId}`;
}

/**
 * Appends an entry to `conversationId` in the commit `tx` under `requestId`, as a passive write's
 * submission records it, for a run's opening: written in the commit that creates the
 * conversation, where pi forbids reading its tables, or with the reads that decide it.
 */
export async function appendUnderRequest(
  tx: Tx,
  conversationId: ConversationId,
  requestId: string,
  append: () => Promise<EntryRecord>,
): Promise<void> {
  const entry = await append();
  await tx.createSubmission({
    conversationId,
    requestId,
    type: "write",
    status: "done",
    entry: entry.id,
  });
}

/**
 * Writes the run `runId`'s closing, the job's end or its pause, to `conversation` as
 * a `closing` entry with `data`, once: a run that has closed keeps its closing. A write of
 * only a kind and data is appended at once, also while the agent is working.
 */
export async function submitClosing<D extends JobSuccess | JobFailure | JobEnding | JobPause>(
  conversation: Conversation,
  runId: string,
  closing: Entry<D>,
  data: NoInfer<D>,
): Promise<void> {
  await conversation.submit(
    { type: "write", entry: { kind: closing.kind, data }, requestId: closingRequest(runId) },
    BACKGROUND_CONTEXT,
  );
}

/**
 * Whether `id` can be an entry ID: pi's IDs are positive integers from one counter that entries,
 * conversations and the rest share, and pi-durable exports no constructor for them.
 */
function isEntryId(id: number): id is EntryId {
  return Number.isSafeInteger(id) && id > 0;
}

/** A conversation of the journal and its latest entry, which a fork without its own inherits. */
export interface ConversationHead {
  id: ConversationId;
  lastEntryId: EntryId | undefined;
}

/** How many records one scan reads at a time. */
const PAGE_SIZE = 256;

/**
 * The journal's events after `after`, in ID order, up to about `maxBytes` of JSON Lines and at least
 * one when there is any, so an event larger than `maxBytes` still travels, alone; with their size,
 * whether more follow them, and the journal's conversations with each one's latest entry. Read in
 * one commit, so the events are those of one consistent state; it writes nothing.
 *
 * Entry and conversation IDs come from one counter, in commit order, so merging by ID gives the
 * journal's order. Each conversation's scan is fork-aware: a fork's scan includes its parent's
 * entries up to the fork point, which are the parent's events. A conversation's own entries come
 * after its record, so starting its scan after its own ID leaves only them.
 */
export function readEvents(
  journal: Session,
  after: number,
  maxBytes: number,
): Promise<{
  events: JobEvent[];
  bytes: number;
  more: boolean;
  conversations: ConversationHead[];
}> {
  return journal.commit(async (tx) => {
    const conversations: ConversationRecord[] = [];
    let cursor;
    do {
      const page = await tx.scanConversations({ order: "ascending" }, PAGE_SIZE, cursor);
      conversations.push(...page.items);
      cursor = page.next;
    } while (cursor !== undefined);

    const events: JobEvent[] = [];
    const heads: ConversationHead[] = [];
    // Reading stops at the lowest ID a conversation has left unread, so the merge has no gap.
    let horizon = Number.POSITIVE_INFINITY;
    for (const conversation of conversations) {
      const latest = await tx.scanEntries(
        { conversationId: conversation.id, order: "descending" },
        1,
      );
      heads.push({ id: conversation.id, lastEntryId: latest.items[0]?.id });
      if (conversation.id > after) {
        events.push({ id: conversation.id, type: "conversation", record: conversation });
      }
      const minEntryId = Math.max(after, conversation.id) + 1;
      if (!isEntryId(minEntryId)) throw new RangeError(`${minEntryId} is not a journal ID`);
      const page = await tx.scanEntries(
        { conversationId: conversation.id, minEntryId, order: "ascending" },
        PAGE_SIZE,
      );
      for (const entry of page.items) events.push({ id: entry.id, type: "entry", record: entry });
      const last = page.items.at(-1);
      if (page.next !== undefined && last) horizon = Math.min(horizon, last.id);
    }

    const sorted = Array.sortWith(events, ({ id }) => id, Order.Number);
    const read: JobEvent[] = [];
    let bytes = 0;
    for (const event of sorted) {
      if (event.id > horizon) break;
      const size = Buffer.byteLength(JSON.stringify(event)) + 1;
      if (bytes + size > maxBytes && read.length > 0) break;
      read.push(event);
      bytes += size;
    }
    const more = read.length < sorted.length || horizon < Number.POSITIVE_INFINITY;
    return { events: read, bytes, more, conversations: heads };
  }, BACKGROUND_CONTEXT);
}

/**
 * What each of `conversations` doing anything is doing, from pi's live state, and when the
 * Runner first saw each part as it is now: as `seen`, the last read's, gives it for a part still
 * live, or now. Both are keyed by what changes with the part, such as a new attempt or a tool call
 * that starts running. The times live in the Runner's memory, so after it restarts each part gets
 * the time it is first seen again: right for a model or tool call, which is made again, but later
 * than the change for a retry wait or a compaction that carries on. Read outside the journal's
 * commit, where pi's other reads would wait on it forever.
 */
export async function readActivity(
  journal: Harness,
  conversations: readonly ConversationHead[],
  seen: ReadonlyMap<string, number>,
): Promise<{ activity: JobActivity; seen: Map<string, number> }> {
  const now = Date.now();
  const seenNow = new Map<string, number>();
  const changedAt = (key: string) => {
    const at = seen.get(key) ?? now;
    seenNow.set(key, at);
    return isoDate(at);
  };
  const activity: ConversationActivity[] = [];
  for (const { id: conversationId, lastEntryId } of conversations) {
    const state = await journal.snapshot(LiveDoc, conversationId, BACKGROUND_CONTEXT);
    const { generation } = state ?? {};
    const tools = (state?.tools ?? []).flatMap(({ callId, name, status }) =>
      status === "done"
        ? []
        : [
            {
              callId,
              name,
              status,
              changedAt: changedAt(`${conversationId} tool ${callId} ${status}`),
            },
          ],
    );
    const compactions = (state?.compactions ?? []).map(
      ({ taskId, reason, blocking, attempt, retry }) => ({
        reason,
        blocking,
        attempt,
        ...(retry ? { retry: isoRetry(retry) } : {}),
        changedAt: changedAt(
          `${conversationId} compaction ${taskId} ${attempt} ${retry ? "wait" : "run"}`,
        ),
      }),
    );
    if (!generation && tools.length === 0 && compactions.length === 0) continue;
    activity.push({
      conversationId,
      ...(generation
        ? {
            generation: {
              attempt: generation.attempt,
              ...(generation.retry ? { retry: isoRetry(generation.retry) } : {}),
              // pi keeps the attempt's number through the wait after it fails, and counts the next
              // attempt when it starts. Every model call starts at attempt 1, but after the
              // conversation's latest entry, which the last call's answer and tool results moved.
              // That entry is read in the events' commit and the live state after it, so a whole
              // tool round falling between the two keeps the last call's time until the next read.
              changedAt: changedAt(
                `${conversationId} generation ${lastEntryId} ${generation.attempt} ${generation.retry ? "wait" : "call"}`,
              ),
            },
          }
        : {}),
      tools,
      compactions,
    });
  }
  return { activity, seen: seenNow };
}

/**
 * `text` cut to `FAILURE_MESSAGE_MAX_LENGTH`. A cut through a surrogate pair leaves half of it, which
 * `toWellFormed` replaces; fewer UTF-16 units than the maximum are never more characters.
 */
export function failureMessage(text: string): string {
  return text.slice(0, FAILURE_MESSAGE_MAX_LENGTH).toWellFormed();
}

/** pi's wait before another attempt, with its end as an ISO date, as the hub stores it. */
function isoRetry(retry: { at: number; error: string }) {
  return { at: isoDate(retry.at), error: retry.error };
}
