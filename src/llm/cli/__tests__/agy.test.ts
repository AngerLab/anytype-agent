import { describe, expect, it, mock } from "bun:test";
import type { ModelMessage } from "ai";
import { firstValueFrom, of, Subject, toArray } from "rxjs";
import type { SshClient, SshProcess } from "../../../ssh/ssh.client";
import { AgyCliProvider } from "../agy";

function createMockProcess(stdout$: Subject<string>, onKill = mock(() => {})): SshProcess {
  return {
    stdout$: stdout$.asObservable(),
    stderr$: of(),
    exit: Promise.resolve({ code: 0 }),
    kill: onKill,
  } as unknown as SshProcess;
}

describe("AgyCliProvider (Unit Tests)", () => {
  it("1. NDJSON chunking: handles lines split across multiple chunks", async () => {
    const stdout$ = new Subject<string>();
    const spawnMock = mock(async () => createMockProcess(stdout$));
    const mockSshClient = { spawn: spawnMock } as unknown as SshClient;

    const provider = new AgyCliProvider(mockSshClient, "agy");
    const messages: ModelMessage[] = [{ role: "user", content: "Hello" }];

    const emissionsPromise = firstValueFrom(provider.exec(messages).pipe(toArray()));
    await Promise.resolve();

    stdout$.next('{"event": "step_update", "step_update": {"step_index": 0, "state": "ACTIVE", ');
    stdout$.next('"step_type": "tool", "tool_name": "search"}}\n{"event": "result", ');
    stdout$.next('"result": {"status": "SUCCESS", "response": "done"}}\n');
    stdout$.complete();

    const result = await emissionsPromise;

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_0",
          toolName: "search",
          input: {},
        },
      ],
    });
    expect(result[1]).toEqual({
      role: "assistant",
      content: "done",
    });
  });

  it("2. Tool Result mapping: maps DONE and ERROR tool steps to tool role messages", async () => {
    const stdout$ = new Subject<string>();
    const spawnMock = mock(async () => createMockProcess(stdout$));
    const mockSshClient = { spawn: spawnMock } as unknown as SshClient;

    const provider = new AgyCliProvider(mockSshClient, "agy");
    const emissionsPromise = firstValueFrom(
      provider.exec([{ role: "user", content: "test" }]).pipe(toArray()),
    );
    await Promise.resolve();

    // Done tool
    stdout$.next(
      JSON.stringify({
        event: "step_update",
        step_update: {
          step_index: 1,
          state: "DONE",
          step_type: "tool",
          tool_name: "fetch",
          tool_info: { output: "fetched text" },
        },
      }) + "\n",
    );

    // Error tool
    stdout$.next(
      JSON.stringify({
        event: "step_update",
        step_update: {
          step_index: 2,
          state: "ERROR",
          step_type: "tool",
          tool_name: "fetch",
          tool_info: { error: { message: "Network error" } },
        },
      }) + "\n",
    );

    // Result
    stdout$.next(
      JSON.stringify({
        event: "result",
        result: { status: "SUCCESS", response: "all done" },
      }) + "\n",
    );
    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(3);
    expect(result[0]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "fetch",
          output: { type: "text", value: "fetched text" },
        },
      ],
    });
    expect(result[1]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_2",
          toolName: "fetch",
          output: { type: "error-text", value: "Network error" },
        },
      ],
    });
    expect(result[2]).toEqual({
      role: "assistant",
      content: "all done",
    });
  });

  it("3. Non-JSON lines and init events: quietly ignored without dropping stream", async () => {
    const stdout$ = new Subject<string>();
    const spawnMock = mock(async () => createMockProcess(stdout$));
    const mockSshClient = { spawn: spawnMock } as unknown as SshClient;

    const provider = new AgyCliProvider(mockSshClient, "agy");
    const emissionsPromise = firstValueFrom(
      provider.exec([{ role: "user", content: "hi" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next("Warning: SSH key not found in cache\n");
    stdout$.next(JSON.stringify({ event: "init", conversation_id: "conv-123" }) + "\n");
    stdout$.next(
      JSON.stringify({
        event: "result",
        result: { status: "SUCCESS", response: "hello back" },
      }) + "\n",
    );
    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      role: "assistant",
      content: "hello back",
    });
  });

  it("4. Failure result: throws error into stream", async () => {
    const stdout$ = new Subject<string>();
    const spawnMock = mock(async () => createMockProcess(stdout$));
    const mockSshClient = { spawn: spawnMock } as unknown as SshClient;

    const provider = new AgyCliProvider(mockSshClient, "agy");
    const emissionsPromise = firstValueFrom(
      provider.exec([{ role: "user", content: "fail" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next(
      JSON.stringify({
        event: "result",
        result: { status: "FAILURE", error: "Token budget exceeded" },
      }) + "\n",
    );
    stdout$.complete();

    expect(emissionsPromise).rejects.toThrow("agy execution failed: Token budget exceeded");
  });

  it("5. Unsubscribe: triggers local abort controller and cancels process", async () => {
    let capturedSignal: AbortSignal | undefined;
    const stdout$ = new Subject<string>();

    const spawnMock = mock(async (_cmd, _args, opts) => {
      capturedSignal = opts.signal;
      return createMockProcess(stdout$);
    });
    const mockSshClient = { spawn: spawnMock } as unknown as SshClient;

    const provider = new AgyCliProvider(mockSshClient, "agy");
    const sub = provider.exec([{ role: "user", content: "long run" }]).subscribe();

    // Wait for spawn() promise to resolve and inner stdout$ subscription to be active
    await Promise.resolve();

    expect(capturedSignal).toBeDefined();
    expect(capturedSignal?.aborted).toBe(false);

    // Unsubscribe from downstream while process is streaming
    sub.unsubscribe();

    expect(capturedSignal?.aborted).toBe(true);
  });
});
