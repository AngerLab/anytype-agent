import { Inject, Injectable } from "@nestjs/common";
import type { ModelMessage } from "ai";
import isString from "lodash/isString";
import { defer, EMPTY, filter, finalize, map, type Observable, tap } from "rxjs";
import type { ChatMessage } from "../client";
import { AnytypeProxy } from "../client/anytype.proxy";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import { SshClient } from "../ssh/ssh.client";
import { LLM_PROVIDERS } from "./llm.constants";
import { LlmAction, type LlmEvent, LlmProvider, LlmResponse, type LlmRunContext } from "./types";

// TODO: make a proper signed string
type SpaceId = string;
type ChatId = string;
type TriggerId = string;
type BotId = string;

@Injectable()
export class LlmService {
  private readonly toBeMovedToDb: Map<SpaceId, Map<ChatId, Map<TriggerId, ModelMessage[]>>> =
    new Map();

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(LLM_PROVIDERS) private readonly providers: LlmProvider[],
    private readonly proxy: AnytypeProxy,
  ) {}

  run(ctx: LlmRunContext, messages: ChatMessage[]): Observable<LlmEvent> {
    return defer(() => {
      const { spaceId, abort } = ctx;
      if (abort.aborted) return EMPTY;

      // TODO: implement fallback mechanism
      const targetProvider = this.providers[0];

      const systemMessage: ModelMessage = { role: "system", content: "" };
      let alias: string | undefined;

      if (targetProvider?.mode === "cli") {
        const port = this.getPortForCli(targetProvider);
        alias = this.proxy.issueSpaceAlias(spaceId);

        systemMessage.content = buildCliPrompt(
          this.config.ANYTYPE.BOT_NAME,
          port,
          this.proxy.routesDoc,
          alias,
        );
      }

      if (!targetProvider) throw new Error("No target provider");

      const history = this.getHistory(ctx, messages);

      return targetProvider?.exec([systemMessage, ...history], abort).pipe(
        tap((response) => this.save(ctx, response)),
        map((response) => {
          if (response.role === "tool") return LlmAction.create(JSON.stringify(response.content));
          if (response.role === "assistant" && isString(response.content))
            return LlmResponse.create(response.content);

          return null;
        }),
        filter((event): event is LlmEvent => event !== null),
        finalize(() => {
          if (alias) this.proxy.revokeSpaceAlias(alias);
        }),
      );
    });
  }

  // TODO: think about a better way to do this
  private getPortForCli(provider: LlmProvider): number {
    if (provider.process instanceof SshClient && provider.process.forwardPort?.remotePort) {
      return provider.process.forwardPort.remotePort;
    }

    return this.proxy.port ?? 0;
  }

  private save({ spaceId, chatId, triggerId }: LlmRunContext, message: ModelMessage): void {
    let spaceMap = this.toBeMovedToDb.get(spaceId);
    // biome-ignore lint/suspicious/noAssignInExpressions: Simple one-liner
    if (!spaceMap) this.toBeMovedToDb.set(spaceId, (spaceMap = new Map()));

    let chatMap = spaceMap.get(chatId);
    // biome-ignore lint/suspicious/noAssignInExpressions: Simple one-liner
    if (!chatMap) spaceMap.set(chatId, (chatMap = new Map()));

    let triggerHistory = chatMap.get(triggerId);
    // biome-ignore lint/suspicious/noAssignInExpressions: Simple one-liner
    if (!triggerHistory) chatMap.set(triggerId, (triggerHistory = []));

    triggerHistory.push(message);
  }

  private readonly getHistory = (
    { spaceId, chatId, botId }: LlmRunContext,
    messages: ChatMessage[],
  ): ModelMessage[] => {
    const chatTurns = this.toBeMovedToDb.get(spaceId)?.get(chatId);
    return messages.flatMap((message) => {
      // If it's a bot response, and its parent user turn is already expanded with tool calls — skip
      if (message.reply_to_message_id && chatTurns?.has(message.reply_to_message_id)) return [];

      const modelMessage = this.mapChatToModel(message, botId);
      const savedTurn = chatTurns?.get(message.id);

      // If there are tool calls for user message
      if (savedTurn) return [modelMessage, ...savedTurn];

      return [modelMessage];
    });
  };

  private readonly mapChatToModel = (message: ChatMessage, botId: BotId): ModelMessage => {
    return {
      role: this.isBotId(botId, message.creator) ? "assistant" : "user",
      content: message.content?.text ?? "",
    };
  };

  private readonly isBotId = (botId: BotId, wire?: string): boolean =>
    Boolean(wire?.endsWith(`_${botId}`));
}

// TODO: come up with a better place for this
function buildCliPrompt(botName: string, port: number, docs: string, spaceAlias: string): string {
  return `You are the AI assistant "${botName}" in Anytype.

Instructions for interacting with Anytype:
- To read or modify data, use the local REST API proxy: http://localhost:${port}
- You have access to the following APIs:
${docs}
- Authorization: Already configured in the proxy server; Bearer tokens and required headers are automatically injected.
- Current Space ID: ${spaceAlias || "unknown"}

Communication & Language Guidelines:
- ALWAYS respond in the exact same language used by the user in their message or query (e.g., if the user wrote in Russian, answer in Russian; if in English, answer in English).
- Formulate a helpful, structured, and clear response.
- Never show the user SPACE ID;
- Do not use Markdown in your responses.
`;
}
