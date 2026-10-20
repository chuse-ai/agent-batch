import { once } from "node:events";

import { createModels, type JsonObject, type Message } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxProvider,
  type FauxResponseFactory,
  fauxText,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";

// The scripted model Runners run on in tests. It imports nothing of the test runner, so a Runner in
// a process of its own can use it too.

/**
 * A scripted model for a Runner's jobs, through pi-ai's faux provider: a test gives `faux` the
 * responses the model makes, in order, across all jobs; with none left, a job's model call fails.
 */
export function scriptedModel() {
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux.provider);
  const { provider, id } = faux.getModel();
  return { faux, models, model: { provider, modelId: id, thinkingLevel: "off" as const } };
}

/** The model's call of `tool` with `args`. */
export function callTool(tool: string, args: JsonObject) {
  return fauxAssistantMessage(fauxToolCall(tool, args), { stopReason: "toolUse" });
}

/** The model's reply in prose, which ends its turn. */
export function reply(prose: string) {
  return fauxAssistantMessage(fauxText(prose));
}

/** A model call that ends only when the Runner aborts it, as a provider's request does. */
export const untilAborted: FauxResponseFactory = async (_context, options) => {
  if (options?.signal) await once(options.signal, "abort");
  return reply("Too late.");
};

/** The text of the last tool result the model was given, and whether it was an error. */
export function lastToolResult(messages: Message[]) {
  const result = messages.findLast((message) => message.role === "toolResult");
  if (!result) throw new Error("The model was given no tool result");
  return {
    text: result.content.map((part) => (part.type === "text" ? part.text : "")).join(""),
    isError: result.isError,
  };
}
