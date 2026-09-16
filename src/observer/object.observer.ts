import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import {
  catchError,
  defer,
  EMPTY,
  exhaustMap,
  filter,
  finalize,
  from,
  map,
  mergeMap,
  type Observable,
  of,
  retry,
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
import { AbstractLlmService, LlmEmptyResponseError, LlmResponse } from "../llm/types";
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

const INITIAL_PROGRESS_TEXT = "> ⏳ Working...";
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
    const pollIntervalMs = 30 * 1000;
    this.logger.log(
      `🚀 [${this.spaceId}] ObjectObserver started for bot "${this.botName}" (participantId: ${this.getBotParticipantId()})`,
    );

    return timer(0, pollIntervalMs).pipe(
      exhaustMap(() => this.scanForMentions()),
      takeUntil(this.destroy$),
    );
  }

  private getObjectsWithMentions(): Observable<ObjectWithBody[]> {
    this.logger.debug(
      `[${this.spaceId}] Fetching bot participant object ${this.getBotParticipantId()}...`,
    );

    return this.request$(() =>
      this.anytype.getObject(this.spaceId, this.getBotParticipantId()),
    ).pipe(
      switchMap((botMember) => {
        const backlinksProp = botMember.properties?.find((p) => p.key === "backlinks");
        const rawIds = (backlinksProp?.objects ?? []) as string[];
        this.logger.log(
          `[${this.spaceId}] Bot participant has ${rawIds.length} backlink(s): ${JSON.stringify(rawIds)}`,
        );

        const backlinkObjectIds = [...new Set(rawIds)].filter(
          (id) => !this.inFlightObjectIds.has(id),
        );

        if (backlinkObjectIds.length === 0) {
          this.logger.debug(
            `[${this.spaceId}] No pending objects to check (in-flight: ${this.inFlightObjectIds.size})`,
          );
          return of([]);
        }

        this.logger.log(
          `[${this.spaceId}] Fetching bodies for ${backlinkObjectIds.length} candidate object(s)...`,
        );

        return from(backlinkObjectIds).pipe(
          mergeMap((id) =>
            this.request$(() => this.anytype.getObject(this.spaceId, id, "md")).pipe(
              catchError((err) => {
                this.logger.warn(
                  `[${this.spaceId}] Failed to fetch backlink object ${id}: ${this.sanitizeErrorMessage(err)}`,
                );
                return EMPTY;
              }),
            ),
          ),
          toArray(),
          map((objects) => {
            const mentionRegex = this.getBotMention();
            return objects.filter((obj) => {
              const hasMarkdown = Boolean(obj?.markdown);
              const matches = hasMarkdown && mentionRegex.test(obj.markdown!);
              this.logger.log(
                `[${this.spaceId}] Object ${obj.id} mention check: ${matches ? "MATCH" : "NO MATCH"} (regex: ${mentionRegex.source})`,
              );
              if (!matches && hasMarkdown) {
                this.logger.debug(
                  `[${this.spaceId}] Object ${obj.id} preview: ${obj.markdown?.slice(0, 100).replace(/\n/g, "\\n")}`,
                );
              }
              return matches;
            });
          }),
        );
      }),
      catchError((err) => {
        this.logger.error(
          `[${this.spaceId}] Error checking mentions: ${this.sanitizeErrorMessage(err)}`,
        );
        return of([]);
      }),
    );
  }

  private scanForMentions(): Observable<unknown> {
    this.logger.debug(`🔎 [${this.spaceId}] Scanning for mentions...`);

    return this.getObjectsWithMentions().pipe(
      mergeMap((objects) => from(objects)),
      mergeMap((obj) =>
        this.handleObjectTrigger(obj).pipe(
          catchError((err) => {
            this.logger.error(
              `[${this.spaceId}] Failed to process object ${obj.id}: ${this.sanitizeErrorMessage(err)}`,
            );
            return EMPTY;
          }),
        ),
      ),
    );
  }

  private handleObjectTrigger(object: ObjectWithBody): Observable<unknown> {
    const rawMarkdown = object?.markdown ?? "";
    this.logger.log(`⚡ [${this.spaceId}] Processing mention in object ${object.id}...`);

    const updatedMarkdown = this.insertProgressMarker(rawMarkdown);
    if (!updatedMarkdown) {
      this.logger.warn(
        `[${this.spaceId}] Skipping object ${object.id}: progress marker already present or mention line not found`,
      );
      return EMPTY;
    }

    this.inFlightObjectIds.add(object.id);
    this.logger.log(`⏳ [${this.spaceId}] Posting progress marker to object ${object.id}...`);

    return this.updateObjectMarkdown(object.id, updatedMarkdown).pipe(
      switchMap(() => {
        const abortController = new AbortController();
        this.logger.log(
          `🤖 [${this.spaceId}] Progress marker posted. Triggering LLM run for object ${object.id}...`,
        );

        return this.processLlmRun(object.id, rawMarkdown, abortController.signal).pipe(
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
    this.logger.log(`💬 [${this.spaceId}] Generating LLM response for object ${objectId}...`);

    return this.llm.run(this.spaceId, { markdown }, signal).pipe(
      filter((e): e is LlmResponse => e instanceof LlmResponse),
      // If the stream completes without any LlmResponse, throw error immediately
      throwIfEmpty(() => new LlmEmptyResponseError()),
      map((res) => {
        const text = res.text.trim();
        if (!text) throw new LlmEmptyResponseError();
        return text;
      }),
    );
  }

  private applyLlmAnswer(objectId: string, rawAnswer: string): Observable<unknown> {
    this.logger.log(`📝 [${this.spaceId}] Applying LLM answer to object ${objectId}...`);
    const formattedAnswer = [
      `> 🤖 **${this.botName}**`,
      ...rawAnswer.split("\n").map((line) => `> ${line}`),
    ].join("\n");

    return this.saveProgressResult(objectId, formattedAnswer);
  }

  private handleLlmError(objectId: string, err: unknown): Observable<unknown> {
    const errMsg = this.sanitizeErrorMessage(err);
    this.logger.error(`❌ [${this.spaceId}] LLM run failed for object ${objectId}: ${errMsg}`);

    const errorText = `⚠️ ${errMsg}`;

    return this.saveProgressResult(objectId, errorText).pipe(catchError(() => EMPTY));
  }

  private insertProgressMarker(markdown: string): string | null {
    if (markdown.includes(INITIAL_PROGRESS_TEXT)) return null;

    const lines = markdown.split("\n");
    const mentionRegex = this.getBotMention();
    const mentionIndex = lines.findIndex((line) => mentionRegex.test(line));
    if (mentionIndex === -1) return null;

    lines.splice(mentionIndex + 1, 0, "", INITIAL_PROGRESS_TEXT);

    return lines.join("\n");
  }

  private saveProgressResult(objectId: string, targetText: string): Observable<unknown> {
    this.logger.log(
      `💾 [${this.spaceId}] Saving progress result and stripping mention links for object ${objectId}...`,
    );

    return this.request$(() => this.anytype.getObject(this.spaceId, objectId, "md")).pipe(
      switchMap((latest) => {
        if (!latest?.markdown) {
          this.logger.warn(
            `[${this.spaceId}] Object ${objectId} markdown is empty, skipping result save`,
          );
          return EMPTY;
        }

        const updatedMarkdown = latest.markdown
          .replace(INITIAL_PROGRESS_TEXT, targetText)
          .replace(new RegExp(this.getBotMention().source, "gi"), `**@${this.botName}**`);

        return this.updateObjectMarkdown(objectId, updatedMarkdown);
      }),
      tap({
        next: () =>
          this.logger.log(`✅ [${this.spaceId}] Successfully updated object ${objectId}!`),
      }),
    );
  }

  private updateObjectMarkdown(objectId: string, markdown: string): Observable<ObjectWithBody> {
    return this.request$(() => this.anytype.updateObject(this.spaceId, objectId, { markdown }));
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
    const normalizedSpaceId = this.spaceId.replaceAll(".", "_");
    return `_participant_${normalizedSpaceId}_${this.botMemberId}`;
  }

  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  private getBotMention(): RegExp {
    // Matches [@BotName](anytype://...) or [BotName](anytype://...)
    return new RegExp(
      `\\[\\s*(?:@)?${this.escapeRegex(
        this.botName,
      )}\\s*\\]\\(anytype:\\/\\/object\\?objectId=_participant_[^)]+\\)`,
      "i",
    );
  }

  private readonly sanitizeErrorMessage = (err: unknown): string => {
    const raw = err instanceof Error ? err.message : String(err);
    return raw.split("\n")[0]?.slice(0, 200) ?? "";
  };
}
