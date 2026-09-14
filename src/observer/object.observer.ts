import { createHash } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import {
  catchError,
  concatMap,
  connect,
  defer,
  EMPTY,
  exhaustMap,
  filter,
  finalize,
  firstValueFrom,
  from,
  ignoreElements,
  type MonoTypeOperatorFunction,
  map,
  merge,
  mergeMap,
  type Observable,
  of,
  retry,
  scan,
  switchMap,
  takeUntil,
  tap,
  throwIfEmpty,
  timeout,
  timer,
  toArray,
} from "rxjs";
import type { AppConfig } from "../app.config";
import { AnytypeService, type ObjectWithBody } from "../client";
import { LLM_SERVICE } from "../llm/llm.module";
import {
  AbstractLlmService,
  LlmAction,
  LlmEmptyResponseError,
  type LlmEvent,
  LlmResponse,
} from "../llm/types";
import { AbstractObserver, type ObserverFactory } from "./types";

@Injectable()
export class ObjectObserverFactory implements ObserverFactory {
  constructor(
    private readonly anytype: AnytypeService,
    @Inject(LLM_SERVICE) private readonly llm: AbstractLlmService,
    private readonly config: ConfigService<AppConfig, true>,
  ) {}

  create(spaceId: string, botName: string, botMemberId: string): ObjectObserver {
    return new ObjectObserver(spaceId, botName, botMemberId, this.anytype, this.llm, this.config);
  }
}

const INITIAL_PROGRESS_TEXT = "⏳ Working...";
const PROGRESS_POST_TIMEOUT_MS = 5_000;
const PROGRESS_RETRY_DELAY_MS = 1_000;
const SAFE_HTTP_TIMEOUT_MS = 15_000;

export class ObjectObserver extends AbstractObserver {
  private readonly inFlightObjectIds = new Set<string>();

  constructor(
    private readonly spaceId: string,
    private readonly botName: string,
    private readonly botMemberId: string,
    private readonly anytype: AnytypeService,
    private readonly llm: AbstractLlmService,
    private readonly config: ConfigService<AppConfig, true>,
  ) {
    super();
  }

  run(): Observable<unknown> {
    // const pollIntervalMs = this.config.get("OBSERVER_SCAN_INTERVAL_MS");
    const pollIntervalMs = 60 * 1000;

    return timer(0, pollIntervalMs).pipe(
      exhaustMap(() => from(this.scanForMentions())),
      takeUntil(this.destroy$),
    );
  }

  private handleProgressMarker(markdown: string): string | null {
    if (markdown.includes(INITIAL_PROGRESS_TEXT)) {
      return null;
    }
    const lines = markdown.split("\n");
    const mentionIndex = lines.findIndex((line) => this.getBotMention().test(line));
    if (mentionIndex === -1) return null;

    lines.splice(mentionIndex + 1, 0, "", INITIAL_PROGRESS_TEXT);

    return lines.join("\n");
  }

  private getBotBacklinkIds(): Observable<string[]> {
    return from(this.anytype.getObject(this.spaceId, this.getBotParticipantId())).pipe(
      map((botMember) => {
        const backlinksProp = botMember.properties?.find((p) => p.key === "backlinks");
        console.log("backlinksProp", backlinksProp);
        return backlinksProp?.objects ?? [];
      }),
    );
  }

  private getObjectsWithMentions(): Observable<ObjectWithBody[]> {
    return this.getBotBacklinkIds().pipe(
      switchMap((linkIds) => {
        return from(linkIds).pipe(
          mergeMap((id) => this.anytype.getObject(this.spaceId, id, "md")),
          toArray(),
          map((objects) =>
            objects.filter((obj) => obj?.markdown && this.getBotMention().test(obj.markdown)),
          ),
        );
      }),
    );
  }

  private scanForMentions(): Observable<unknown> {
    return this.getObjectsWithMentions().pipe(
      mergeMap((objects) => from(objects)),
      filter((obj) => Boolean(obj.markdown) && !this.inFlightObjectIds.has(obj.id)),
      mergeMap((obj) =>
        this.handleObjectTrigger(obj).pipe(
          catchError((err) => {
            this.logger.error(`Failed to process object ${obj.id}: ${err.message}`);
            return EMPTY;
          }),
        ),
      ),
    );
  }

  private updateObjectMarkdown(objectId: string, markdown: string): Observable<ObjectWithBody> {
    return this.request$(() => this.anytype.updateObject(this.spaceId, objectId, { markdown }));
  }

  private handleObjectTrigger(object: ObjectWithBody): Observable<unknown> {
    const updatedMarkdown = this.handleProgressMarker(object?.markdown ?? "");
    if (!updatedMarkdown) {
      return EMPTY;
    }

    this.inFlightObjectIds.add(object.id);

    return this.updateObjectMarkdown(object.id, updatedMarkdown).pipe(
      switchMap(() => {
        const abortController = new AbortController();

        return this.processLlmRun(object.id, updatedMarkdown, abortController.signal).pipe(
          switchMap((rawAnswer) => this.applyLlmAnswer(object.id, rawAnswer)),
          catchError((err) => {
            if (!abortController.signal.aborted) abortController.abort();
            return this.handleLlmError(object.id, err);
          }),
          tap({
            unsubscribe: () => {
              if (!abortController.signal.aborted) abortController.abort();
            },
          }),
          finalize(() => {
            this.inFlightObjectIds.delete(object.id);
          }),
        );
      }),
    );
  }

  private processLlmRun(
    objectId: string,
    markdown: string,
    signal: AbortSignal,
  ): Observable<string> {
    this.logger.log(`💬 Generating LLM response for object ${objectId}...`);

    return this.llm.run(this.spaceId, { markdown }, signal).pipe(
      filter((e): e is LlmResponse => e instanceof LlmResponse),
      map((res) => {
        const text = res.text.trim();
        if (!text) throw new LlmEmptyResponseError();
        return text;
      }),
    );
  }

  private applyLlmAnswer(objectId: string, rawAnswer: string): Observable<unknown> {
    // defer?
    return defer(async () => {
      const latestObject = await this.anytype.getObject(this.spaceId, objectId, "md");
      if (!latestObject?.markdown) return;

      const formattedAnswer = [
        `> 🤖 **${this.botName}**`,
        ...rawAnswer.split("\n").map((line) => `> ${line}`),
      ].join("\n");

      const updatedMarkdown = this.editProgressMarker(latestObject.markdown, formattedAnswer);

      await this.anytype.updateObject(this.spaceId, objectId, { markdown: updatedMarkdown });
    });
  }

  private editProgressMarker(markdown: string, targetText: string): string {
    return markdown
      .replace(INITIAL_PROGRESS_TEXT, targetText)
      .replace(new RegExp(this.getBotMention().source, "g"), `**@${this.botName}**`);
  }

  private handleLlmError(objectId: string, err: unknown): Observable<unknown> {
    const errMsg = this.sanitizeErrorMessage(err);
    this.logger.error(`❌ LLM run failed: ${errMsg}`);

    const errorText = `⚠️ ${errMsg}`;

    return defer(async () => {
      const latest = await this.anytype.getObject(this.spaceId, objectId, "md").catch(() => null);
      if (!latest?.markdown) return;

      const errorMarkdown = this.editProgressMarker(latest.markdown, errorText);

      await this.anytype
        .updateObject(this.spaceId, objectId, { markdown: errorMarkdown })
        .catch(() => null);
    });
  }

  private request$<T>(
    factory: () => Promise<T>,
    options?: { timeoutMs?: number; retryCount?: number; retryDelayMs?: number },
  ): Observable<T> {
    const timeoutMs = options?.timeoutMs ?? SAFE_HTTP_TIMEOUT_MS;
    const retryCount = options?.retryCount ?? 0;

    return defer(factory).pipe(
      timeout(timeoutMs),
      retryCount > 0
        ? retry({ count: retryCount, delay: options?.retryDelayMs ?? PROGRESS_RETRY_DELAY_MS })
        : (source$) => source$,
    );
  }

  private getBotParticipantId(): string {
    return `_participant_${this.spaceId}_${this.botMemberId}`;
  }

  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  private getBotMention(): RegExp {
    // Matches [@BotName](anytype://...) or [BotName](anytype://...)
    return new RegExp(
      `\\[(?:@)?${this.escapeRegex(
        this.botName,
      )}\\]\\(anytype:\\/\\/object\\?objectId=_participant_[^)]+\\)`,
      "g",
    );
  }

  private readonly sanitizeErrorMessage = (err: unknown): string => {
    const raw = err instanceof Error ? err.message : String(err);
    return raw.split("\n")[0]?.slice(0, 200) ?? "";
  };
}
