import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
  type ConversationId,
  type ConversationRecord,
  defineEntry,
  type EntryId,
  type Harness,
  LiveDoc,
  type Session,
} from "@earendil-works/pi-durable";
import {
  type ConversationActivity,
  isoDate,
  JOB_ENDED,
  JOB_RECOVERED,
  JOB_STARTED,
  type JobActivity,
  type JobEnding,
  type JobEvent,
  type JobOpening,
} from "contract";
import { Array, Order } from "effect";

import runnerPackage from "../package.json" with { type: "json" };

// What the Runner records in a job's journal, and how it reads the journal's events for the hub.
// The journal's entries and conversation records become the job's events; our own entries
// record the job's start, its recovery after a Runner restart, and its end with its result, and the
// hub reads the job's lifecycle from them. Their kinds and data are the contract's: kinds name their
// subject in the past tense under `agent-batch.`, and each carries its time as `timestamp`, Unix
// milliseconds by the Runner's clock, as pi-ai's messages do.

/**
 * The Runner's version, which each entry written on opening a journal carries: each release fixes
 * pi's exact version, so it identifies the pi that wrote what follows.
 */
export const RUNNER_VERSION: string = runnerPackage.version;

/** Written once, in the commit that creates the job's conversation, when its agent first starts. */
export const JobStarted = defineEntry<JobOpening>(JOB_STARTED);

/** Written each time a restarted Runner reopens the job and it carries on from its journal. */
export const JobRecovered = defineEntry<JobOpening>(JOB_RECOVERED);

/** Written once, when the job's agent has ended, with its result when it succeeded. */
export const JobEnded = defineEntry<JobEnding>(JOB_ENDED);

/** The request ID that writes `JobEnded` once, and by which a reopened journal finds it. */
export const JOB_ENDED_REQUEST = JobEnded.kind;

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

/** pi's wait before another attempt, with its end as an ISO date. */
function isoRetry(retry: { at: number; error: string }) {
  return { at: isoDate(retry.at), error: retry.error };
}
