export { HubConnection } from "./hub-connection.ts";

export default {
  async fetch(request, env): Promise<Response> {
    const connection = env.HUB_CONNECTION.getByName("hub");
    const { pathname } = new URL(request.url);
    if (request.method === "POST" && pathname === "/start") {
      await connection.start();
      return new Response(null, { status: 204 });
    }
    if (request.method === "POST" && pathname === "/stop") {
      await connection.stop();
      return new Response(null, { status: 204 });
    }
    return new Response("Not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
