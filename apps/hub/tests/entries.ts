import type { JobEvent } from "contract";

/** The kinds of the entries among `events`, in order. */
export function entryKinds(events: readonly JobEvent[]) {
  return events.filter((event) => event.type === "entry").map((event) => event.record.kind);
}

/** The kinds of the Runner's own entries among `events`, in order: the job's lifecycle. */
export function lifecycleOf(events: readonly JobEvent[]) {
  return entryKinds(events).filter((kind) => kind.startsWith("agent-batch."));
}
