import { expect, test } from "bun:test";
import { defaultConfig } from "../src/config";
import { responseRequest, startServer } from "../src/server";

function requestBody(turnId: string, extra: Record<string, unknown> = {}) {
  return {
    model: "chatgpt-web/luna",
    stream: false,
    metadata: { turn_id: turnId, thread_id: "thread_external_provider_e2e" },
    input: [{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "Continue the isolated provider test" }],
      internal_chat_message_metadata_passthrough: { turn_id: turnId },
    }],
    ...extra,
  };
}

test("external provider serves models, streaming responses, tools, continuation, and errors", async () => {
  const config = {
    ...defaultConfig("browser-only"),
    codexIntegrationMode: "external-provider" as const,
    solAvailable: false,
    proAvailable: false,
    port: 0,
  };
  let observedContinuation = false;
  const server = startServer(config, {
    fetchUpstream: async () => Response.json({ models: [{
      slug: "gpt-5.6-sol",
      display_name: "5.6 Sol",
      visibility: "list",
      supported_in_api: true,
      supported_reasoning_levels: [],
      tool_mode: "code_mode_only",
    }] }),
    responseAdapterFactory: () => ({
      name: "isolated-e2e",
      async runTurn(parsed, _incoming, emit) {
        const rawBody = parsed._rawBody as Record<string, any>;
        if (rawBody.previous_response_id) observedContinuation = true;
        if (rawBody.metadata?.error_test === true) throw new Error("isolated adapter failure");
        if (parsed.context.tools?.length) {
          emit({ type: "tool_call_start", id: "call_external", name: "exec" });
          emit({ type: "tool_call_delta", arguments: JSON.stringify({ input: "text('external')" }) });
          emit({ type: "tool_call_end" });
        }
        emit({ type: "text_delta", text: "isolated provider response" });
        emit({ type: "done", endTurn: true });
      },
    }),
  });
  const endpoint = `http://127.0.0.1:${server.port}`;
  try {
    expect((await fetch(`${endpoint}/healthz`)).status).toBe(200);
    expect((await fetch(`${endpoint}/v1/models`, { headers: { authorization: "Bearer isolated" } })).status).toBe(200);

    const response = await fetch(`${endpoint}/v1/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(requestBody("turn_external_response")),
    });
    expect(response.status).toBe(200);
    const responseBody = await response.json() as { id?: string };
    expect(JSON.stringify(responseBody)).toContain("isolated provider response");
    expect(typeof responseBody.id).toBe("string");

    const streamed = await responseRequest(
      new Request("http://127.0.0.1:17841/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ...requestBody("turn_external_stream"), stream: true }),
      }),
      config,
      () => ({
        name: "isolated-e2e-stream",
        async runTurn(_parsed, _incoming, emit) {
          emit({ type: "text_delta", text: "isolated streaming response" });
          emit({ type: "done", endTurn: true });
        },
      }),
    );
    const streamText = await streamed.text();
    expect(streamed.status).toBe(200);
    expect(streamText).toContain("isolated streaming response");
    expect(streamText).toContain("[DONE]");

    const toolResponse = await fetch(`${endpoint}/v1/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ...requestBody("turn_external_tool"),
        tools: [{ type: "function", name: "exec", description: "run", parameters: { type: "object" } }],
      }),
    });
    expect(toolResponse.status).toBe(200);
    expect(JSON.stringify(await toolResponse.json())).toContain("call_external");

    const continuation = await fetch(`${endpoint}/v1/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(requestBody("turn_external_continuation", { previous_response_id: responseBody.id })),
    });
    expect(continuation.status).toBe(200);
    expect(observedContinuation).toBe(true);

    const errorResponse = await fetch(`${endpoint}/v1/responses`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ...requestBody("turn_external_error"),
        metadata: { ...requestBody("turn_external_error").metadata, error_test: true },
      }),
    });
    expect(errorResponse.status).toBe(200);
    expect(JSON.stringify(await errorResponse.json())).toContain("isolated adapter failure");
  } finally {
    await server.stop(true);
  }
});

test("external provider propagates client cancellation to the adapter", async () => {
  const config = {
    ...defaultConfig("browser-only"),
    codexIntegrationMode: "external-provider" as const,
    solAvailable: false,
    proAvailable: false,
    port: 0,
  };
  let aborted = false;
  const server = startServer(config, {
    responseAdapterFactory: () => ({
      name: "isolated-e2e-cancel",
      async runTurn(_parsed, incoming) {
        await new Promise<void>(resolve => incoming.abortSignal?.addEventListener("abort", () => {
          aborted = true;
          resolve();
        }, { once: true }));
      },
    }),
  });
  const controller = new AbortController();
  try {
    const pending = fetch(`http://127.0.0.1:${server.port}/v1/responses`, {
      method: "POST",
      signal: controller.signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...requestBody("turn_external_cancel"), stream: true }),
    });
    const response = await pending;
    controller.abort();
    await response.body?.cancel().catch(() => undefined);
    await new Promise(resolve => setTimeout(resolve, 25));
    expect(aborted).toBe(true);
  } finally {
    await server.stop(true);
  }
});
