import {
  createJSONRPCErrorResponse,
  createJSONRPCNotification,
  JSONRPCClient,
  JSONRPCErrorCode,
  JSONRPCErrorException,
  type JSONRPCID,
  JSONRPCServer,
  JSONRPCServerAndClient,
} from "json-rpc-2.0";
import * as z from "zod/mini";

// JSON-RPC 2.0 over a text socket, typed: a map of methods, each with Zod Mini schemas for its params
// and result, and a map of notifications with their params. A server answers the methods with
// handlers checked against the schemas; a client makes the requests and takes the notifications.
// The library frames, matches and answers; this adds only the schemas.

/** A method's params and result. */
interface Method<Params extends z.ZodMiniType, Result extends z.ZodMiniType> {
  params: Params;
  result: Result;
}

type Methods = Record<string, Method<z.ZodMiniType, z.ZodMiniType>>;
type Notifications = Record<string, z.ZodMiniType>;

// A schema's parsed and accepted values, as `z.output` and `z.input` give them, written as indexed
// types: the dispatch below relates a method's handler to its schemas through a generic method
// name, which TypeScript can do with these and not with Zod's conditional aliases.
type Output<Schema extends z.ZodMiniType> = Schema["_zod"]["output"];
type Input<Schema extends z.ZodMiniType> = Schema["_zod"]["input"];

/** The handler of each method, given its params and the context the receiver passes. */
export type Handlers<M extends Methods, Context> = {
  [Name in keyof M]: (
    params: Output<M[Name]["params"]>,
    context: Context,
  ) => Promise<Input<M[Name]["result"]>>;
};

/** The handler of each notification, given its params. */
export type NotificationHandlers<N extends Notifications> = {
  [Name in keyof N]: (params: Output<N[Name]>) => void;
};

/** Serves method `name` on `server` with `handler`: params and result checked against the schemas. */
function serveMethod<M extends Methods, Context, Name extends keyof M & string>(
  server: JSONRPCServer<Context>,
  name: Name,
  schemas: M[Name],
  handler: Handlers<M, Context>[Name],
): void {
  server.addMethod(name, async (rawParams, context) => {
    const parsed = z.safeParse(schemas.params, rawParams);
    if (!parsed.success) {
      throw new JSONRPCErrorException(
        z.prettifyError(parsed.error),
        JSONRPCErrorCode.InvalidParams,
      );
    }
    return z.parse(schemas.result, await handler(parsed.data, context));
  });
}

/** Serves notification `name` on `server`: its handler runs once the params check against `schema`. */
function serveNotification<N extends Notifications, Name extends keyof N & string>(
  server: JSONRPCServerAndClient,
  name: Name,
  schema: N[Name],
  handler: NotificationHandlers<N>[Name],
): void {
  server.addMethod(name, (rawParams) => {
    const parsed = z.safeParse(schema, rawParams ?? {});
    if (parsed.success) handler(parsed.data);
  });
}

/** The library's answer to an error: a refusal's own code and data, anything else hidden. */
function toErrorResponse(id: JSONRPCID, error: unknown) {
  return error instanceof JSONRPCErrorException
    ? createJSONRPCErrorResponse(id, error.code, error.message, error.data)
    : createJSONRPCErrorResponse(id, JSONRPCErrorCode.InternalError, "Internal error");
}

/**
 * Answers a peer's requests with `handlers`, each run with `context`. Each incoming text message is
 * answered with the text to send back, or nothing for a notification; this side makes no requests
 * through it. Malformed text and what is not a request are answered as JSON-RPC prescribes, by the
 * library. Thrown errors other than refusals answer as a plain internal error, so nothing internal
 * reaches the peer.
 */
export function createJsonRpcServer<M extends Methods, Context>(
  methods: M,
  handlers: Handlers<M, Context>,
) {
  const server = new JSONRPCServer<Context>();
  for (const name in methods) serveMethod(server, name, methods[name], handlers[name]);
  server.mapErrorToJSONRPCErrorResponse = toErrorResponse;
  return async (text: string, context: Context): Promise<string | undefined> => {
    const response = await server.receiveJSON(text, context);
    return response ? JSON.stringify(response) : undefined;
  };
}

/** The text of a notification to a peer. */
export function notification<N extends Notifications, Name extends keyof N & string>(
  notifications: N,
  name: Name,
  params: z.input<N[Name]>,
): string {
  return JSON.stringify(createJSONRPCNotification(name, z.parse(notifications[name], params)));
}

/** The requests this side can make of a peer, and the notifications it takes from it. */
export interface JsonRpcClient<M extends Methods> {
  /** Makes a request, answered within `timeoutMs` or failed. */
  request<Name extends keyof M & string>(
    name: Name,
    params: Input<M[Name]["params"]>,
    timeoutMs: number,
  ): Promise<Output<M[Name]["result"]>>;
  /** Takes a text message from the peer: a response to a request, or a notification. */
  receive(text: string): void;
  /** Fails every request still waiting, for a socket that closed. */
  rejectAllPendingRequests(message: string): void;
}

/**
 * Makes requests of a peer through `send`, matching its responses to them by ID, and passes its
 * notifications to `onNotification`; the library routes each incoming message to one or the other.
 * Responses are checked against the method's result schema.
 */
export function createJsonRpcClient<M extends Methods, N extends Notifications>(
  methods: M,
  notifications: N,
  onNotification: NotificationHandlers<N>,
  send: (text: string) => void,
): JsonRpcClient<M> {
  const server = new JSONRPCServer();
  server.mapErrorToJSONRPCErrorResponse = toErrorResponse;
  const client = new JSONRPCClient<void>((payload) => send(JSON.stringify(payload)));
  const peer = new JSONRPCServerAndClient(server, client);
  for (const name in notifications) {
    serveNotification(peer, name, notifications[name], onNotification[name]);
  }
  return {
    async request<Name extends keyof M & string>(
      name: Name,
      params: Input<M[Name]["params"]>,
      timeoutMs: number,
    ) {
      const raw: unknown = await peer
        .timeout(timeoutMs)
        .request(name, z.parse(methods[name].params, params));
      // Annotated on a local: the method's declared return type is what relates it to the schema.
      const result: Output<M[Name]["result"]> = z.parse(methods[name].result, raw);
      return result;
    },
    receive(text) {
      let message: unknown;
      try {
        message = JSON.parse(text);
      } catch {
        return;
      }
      void peer.receiveAndSend(message);
    },
    rejectAllPendingRequests(message) {
      peer.rejectAllPendingRequests(message);
    },
  };
}
