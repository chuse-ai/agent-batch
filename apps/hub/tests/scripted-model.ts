import { once } from "node:events";

import {
  calculateCost,
  createAssistantMessageEventStream,
  createModels,
  createProvider,
  type JsonObject,
  type Message,
  type SimpleStreamOptions,
  type StreamFunction,
} from "@earendil-works/pi-ai";
import {
  createFauxCore,
  fauxAssistantMessage,
  type FauxResponseFactory,
  fauxText,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";

// The scripted model Runners run on in tests. It imports nothing of the test runner, so a Runner in
// a process of its own can use it too.

/**
 * What the scripted model charges, in US dollars per million tokens: a real model's prices, so a
 * test's model calls stay far below a job's default budget of $1.
 */
const PRICES = { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 };

/**
 * A scripted model for a Runner's jobs, through pi-ai's faux provider: a test gives `faux` the
 * responses the model makes, in order, across all jobs; with none left, a job's model call fails.
 * pi-ai's faux provider prices every call at nothing, so each call is priced as a real provider
 * prices it, from its tokens at `PRICES`, and a job's cost grows as it runs.
 */
export function scriptedModel() {
  const faux = createFauxCore({ models: [{ id: "scripted", cost: PRICES }] });
  const models = createModels();
  models.setProvider(
    createProvider({
      id: faux.provider,
      auth: { apiKey: { name: "Scripted", resolve: async () => ({ auth: {} }) } },
      models: faux.models,
      api: {
        stream: priced(faux.stream),
        streamSimple: priced(faux.streamSimple),
        fetchDeferred: faux.fetchDeferred,
        cancelDeferred: faux.cancelDeferred,
      },
    }),
  );
  const { provider, id } = faux.getModel();
  return { faux, models, model: { provider, modelId: id, thinkingLevel: "off" as const } };
}

/** `stream` with each finished call priced at its model's prices, with pi-ai's `calculateCost`. */
function priced(
  stream: StreamFunction<string, SimpleStreamOptions>,
): StreamFunction<string, SimpleStreamOptions> {
  return (model, context, options) => {
    const source = stream(model, context, options);
    const pricedStream = createAssistantMessageEventStream();
    void (async () => {
      for await (const event of source) {
        if (event.type === "done") {
          event.message.usage.cost = calculateCost(model, event.message.usage);
        }
        pricedStream.push(event);
      }
    })();
    return pricedStream;
  };
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
