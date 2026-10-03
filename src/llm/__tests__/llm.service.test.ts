import { describe, expect, it, mock } from "bun:test";
import type { ModelMessage } from "ai";
import { firstValueFrom, Subject, toArray } from "rxjs";
import type { ChatMessage } from "../../client";
import type { AnytypeProxy } from "../../client/anytype.proxy";
import type { AppConfig } from "../../config/config.schema";
import { LlmService } from "../llm.service";
import { LlmAction, LlmProvider, LlmResponse, type LlmRunContext } from "../types";

class MockProvider extends LlmProvider {
  public readonly mode = "cli";
  public readonly process = null;
  public capturedMessages: ModelMessage[] = [];
  public capturedSignal?: AbortSignal;
  public responseSubject = new Subject<ModelMessage>();

  exec(messages: ModelMessage[], abort?: AbortSignal) {
    this.capturedMessages = messages;
    this.capturedSignal = abort;
    return this.responseSubject.asObservable();
  }
}

describe("LlmService (Unit Tests)", () => {
  const mockConfig = {
    ANYTYPE: {
      BOT_NAME: "TestBot",
      API_URL: "http://mock-api:31012",
    },
  } as unknown as AppConfig;

  const createMockProxy = () => {
    const revoked: string[] = [];
    return {
      port: 45678,
      routesDoc: "GET /v1/spaces/{space_id}/objects",
      issueSpaceAlias: mock((_spaceId: string) => "123456"),
      revokeSpaceAlias: mock((alias: string) => {
        revoked.push(alias);
      }),
      revoked,
    } as unknown as AnytypeProxy & { revoked: string[] };
  };

  const createContext = (overrides: Partial<LlmRunContext> = {}): LlmRunContext => ({
    spaceId: "space-1",
    chatId: "chat-1",
    triggerId: "msg-1",
    botId: "bot123",
    abort: new AbortController().signal,
    ...overrides,
  });

  it("1. CLI lifecycle: issues space alias, passes CLI system prompt, and revokes alias on complete", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const ctx = createContext();
    const messages: ChatMessage[] = [
      {
        id: "msg-1",
        creator: "user_456",
        content: { text: "Hello bot!" },
      },
    ];

    const streamPromise = firstValueFrom(service.run(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    expect(proxy.issueSpaceAlias).toHaveBeenCalledWith("space-1");
    expect(provider.capturedMessages).toHaveLength(2);

    const systemMsg = provider.capturedMessages[0]!;
    expect(systemMsg.role).toBe("system");
    expect(systemMsg.content).toContain('You are the AI assistant "TestBot"');
    expect(systemMsg.content).toContain("http://localhost:45678");
    expect(systemMsg.content).toContain("Current Space ID: 123456");

    const userMsg = provider.capturedMessages[1]!;
    expect(userMsg.role).toBe("user");
    expect(userMsg.content).toBe("Hello bot!");

    // Emit assistant response
    provider.responseSubject.next({
      role: "assistant",
      content: "Hello human!",
    });
    provider.responseSubject.complete();

    const events = await streamPromise;
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual(LlmResponse.create("Hello human!"));
    expect(proxy.revokeSpaceAlias).toHaveBeenCalledWith("123456");
  });

  it("2. Space alias revocation on error: revokes alias even when provider stream errors", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const ctx = createContext();
    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.error(new Error("CLI crash"));

    expect(streamPromise).rejects.toThrow("CLI crash");
    expect(proxy.revokeSpaceAlias).toHaveBeenCalledWith("123456");
  });

  it("3. Mapping & Streaming: maps tool messages to LlmAction and string content to LlmResponse", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const ctx = createContext();
    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "search",
          output: { type: "text", value: "search results" },
        },
      ],
    });
    provider.responseSubject.next({
      role: "assistant",
      content: "Here is your answer.",
    });
    provider.responseSubject.complete();

    const events = await streamPromise;
    expect(events).toHaveLength(2);
    expect(events[0]).toBeInstanceOf(LlmAction);
    expect((events[0] as LlmAction).detail).toContain("search results");
    expect(events[1]).toBeInstanceOf(LlmResponse);
    expect((events[1] as LlmResponse).text).toBe("Here is your answer.");
  });

  it("4. History & Turn State: saves intermediate tool responses into trigger state and expands in subsequent runs", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const ctx1 = createContext({ triggerId: "msg-1" });
    const userMsg1: ChatMessage = {
      id: "msg-1",
      creator: "user_999",
      content: { text: "What's the weather?" },
    };

    // First turn: model produces a tool message and finishes
    const run1Promise = firstValueFrom(service.run(ctx1, [userMsg1]).pipe(toArray()));
    await Promise.resolve();

    const toolMsg: ModelMessage = {
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_w",
          toolName: "weather",
          output: { type: "text", value: "Sunny" },
        },
      ],
    };
    provider.responseSubject.next(toolMsg);
    provider.responseSubject.next({
      role: "assistant",
      content: "It is sunny!",
    });
    provider.responseSubject.complete();
    await run1Promise;

    // Second turn: new user message in same chat
    provider.responseSubject = new Subject<ModelMessage>();
    const ctx2 = createContext({ triggerId: "msg-2" });
    const userMsg2: ChatMessage = {
      id: "msg-2",
      creator: "user_999",
      content: { text: "Thanks!" },
    };

    const run2Promise = firstValueFrom(service.run(ctx2, [userMsg1, userMsg2]).pipe(toArray()));
    await Promise.resolve();

    // Verify history reconstruction:
    // systemMsg [0], userMsg1 [1], toolMsg [2], assistantMsg [3], userMsg2 [4]
    expect(provider.capturedMessages).toHaveLength(5);
    expect(provider.capturedMessages[1]!.content).toBe("What's the weather?");
    expect(provider.capturedMessages[2]).toEqual(toolMsg);
    expect(provider.capturedMessages[3]!.content).toBe("It is sunny!");
    expect(provider.capturedMessages[4]!.content).toBe("Thanks!");

    provider.responseSubject.complete();
    await run2Promise;
  });

  it("5. Bot creator detection: correctly identifies bot messages by suffix", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const ctx = createContext({ botId: "mybot_id" });
    const messages: ChatMessage[] = [
      {
        id: "1",
        creator: "user_123",
        content: { text: "hi" },
      },
      {
        id: "2",
        creator: "prefix_mybot_id",
        content: { text: "hello there" },
      },
      {
        id: "3",
        creator: "other_bot_notmine",
        content: { text: "im someone else" },
      },
    ];

    const streamPromise = firstValueFrom(service.run(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    expect(provider.capturedMessages[1]!.role).toBe("user");
    expect(provider.capturedMessages[2]!.role).toBe("assistant");
    expect(provider.capturedMessages[3]!.role).toBe("user");

    provider.responseSubject.complete();
    await streamPromise;
  });

  it("6. Pre-aborted context returns EMPTY immediately without issuing alias", async () => {
    const provider = new MockProvider();
    const proxy = createMockProxy();
    const service = new LlmService(mockConfig, [provider], proxy);

    const abortController = new AbortController();
    abortController.abort();

    const ctx = createContext({ abort: abortController.signal });
    const events = await firstValueFrom(service.run(ctx, []).pipe(toArray()));

    expect(events).toHaveLength(0);
    expect(proxy.issueSpaceAlias).not.toHaveBeenCalled();
  });
});
