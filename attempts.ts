// private async pollAndProcessMentions(): Promise<void> {
//   try {
//     // 1. Получаем список документов из backlinks бота
//     const botMember = await this.anytype.getObject(this.spaceId, this.getBotId());
//     const backlinksProp = botMember.properties?.find((p) => p.key === "backlinks");
//     const backlinkIds: string[] = backlinksProp?.objects ?? [];

//     if (backlinkIds.length === 0) return;

//     // 2. Скачиваем документы
//     const objects = await Promise.all(
//       backlinkIds.map((id) => this.anytype.getObject(this.spaceId, id, "md").catch(() => null)),
//     );

//     for (const doc of objects) {
//       if (!doc || !doc.markdown) continue;
//       await this.handleDocument(doc);
//     }
//   } catch (err: unknown) {
//     const msg = err instanceof Error ? err.message : String(err);
//     this.logger.error(`Failed to poll mentions: ${msg}`);
//   }
// }

// onApplicationBootstrap(): void {
//   this.sub = timer(0, this.config.get("OBSERVER_SCAN_INTERVAL_MS"))
//     .pipe(switchMap(() => this.checkSpaces()))
//     .subscribe();
// }

// run(): Observable<unknown> {
//   // const ONE_MINUTE_MS = 60 * 1000;
//   // const now = Date.now();
//   // let lastSearchTime = now;
//   // return timer(0, ONE_MINUTE_MS).pipe(
//   //   switchMap(() => {
//   //     return this.getMention().pipe(
//   //       takeUntil(this.destroy$),
//   //     );
//   //   }),
//   // )

//   return this.getMention().pipe(
//     // TODO: Date.now which will be update each search
//     takeUntil(this.destroy$),
//   )
// }

// private extractMentions(object: ObjectWithBody): BotMention[]{
//   const mentionPattern = new RegExp(`\\[${this.botName}\\]\\(anytype:\\/\\/object\\?objectId=_participant_[^)]+\\)`);

//   if (!object.markdown) return [];

//   // const lines = object.markdown
//   //   .split("\n")
//   //   .map((line) => line.trim())
//   //   .filter((line) => line.length > 0);

//   // const mentionLines = lines.filter((line) => mentionPattern.test(line));

//   // return mentionLines.map((line, idx) => ({
//   //   objectId: object.id,
//   //   objectName: object.name || "Untitled",
//   //   contextText: line,
//   //   mentionIndex: idx + 1,
//   // }));
//   //
//    const lines = object.markdown.split("\n");
//     const mentions: BotMention[] = [];

//     for (let i = 0; i < lines.length; i++) {
//       const line = lines[i].trim();
//       if (!mentionPattern.test(line)) continue;

//       const nextLine = (lines[i + 1] ?? "").trim();
//       if (nextLine.includes("↳ [Agent Response:")) continue; // Уже отвечено, пропускаем!

//       mentions.push({
//         objectId: object.id,
//         objectName: object.name || "Untitled",
//         contextText: line,
//         mentionIndex: mentions.length + 1,
//       });
//     }

//     return mentions;
// }
//
//   private getMention(): Observable<ObjectWithBody[]> {
//     const ONE_MINUTE_MS = 60 * 1000;

//     return timer(0, ONE_MINUTE_MS).pipe(
//       switchMap(() =>
//         from(this.anytype.getObject(this.spaceId, this.getBotId())).pipe(
//           map((memberData) => {
//             const backlinksProp = memberData.properties?.find(
//               (prop) => prop.key === "backlinks",
//             );
//             return backlinksProp?.objects ?? [];
//           }),

//           switchMap((currentBacklinks) => {
//             if (currentBacklinks.length === 0) return Promise.resolve([]);
//             return Promise.all(
//               currentBacklinks.map((id) => this.anytype.getObject(this.spaceId, id, "md").catch(() => null)),
//             );
//           }),

//           map((objects) => {
//             const newMentions: BotMention[] = [];

//             for (const obj of objects) {
//               if (!obj || !obj.markdown) continue;

//               // 2. Читаем дату изменения документа
//               const lastModDate = obj.properties?.find((p) => p.key === "last_modified_date")?.
//                 date ?? "";

//               // 3. Если документ не менялся — мы его уже обработали на прошлых тиках
//               if (this.lastModifiedDates.get(obj.id) === lastModDate) continue;

//               // 4. Документ новый или обновлен — достаем неотвеченные меншены
//               const mentions = this.extractMentions(obj);
//               if (mentions.length > 0) newMentions.push(...mentions);

//               // Запоминаем новую дату
//               this.lastModifiedDates.set(obj.id, lastModDate);
//             }

//             return newMentions;
//           }),

//           // map((newObjects) =>
//           //   newObjects.filter((obj): obj is ObjectWithBody => obj !== null),
//           // ),

//           catchError((err) => {
//             this.logger.error(`Failed to poll objects in space ${this.spaceId}:`, err);
//             return of([]);
//           }),
//         ),
//       ),
//       tap((mentionsToProcess) => {
//         if (mentionsToProcess.length > 0)
//           this.logger.log(`Found ${mentionsToProcess.length} fresh mention(s) requiring
// response!`, mentionsToProcess);
//       }),
//       // tap((finalObjects) => {

//       //   console.log(`MENTIONS`,  finalObjects.map((object) => this.extractMentions(object)));

//       //   // console.log(`MENTIONS`, finalObjects);
//       // }),
//     );
//   }

////////end

// private getMention(): Observable<ObjectWithBody[]>  {
//   const oneMinute = 60 * 1000;
//   return timer(0, oneMinute).pipe(
//     switchMap(async () => {

//       const memberData = await this.anytype.getObject(this.spaceId, this.getBotId());
//       const backlinksProp = memberData.properties?.find((prop) => prop.key === "backlinks");
//       const currentBacklinks: string[] = backlinksProp?.objects ?? [];
//       if (currentBacklinks.length === 0) return [];

//       const newObjects = await Promise.all(
//         currentBacklinks.map((id) =>
//           this.anytype.getObject(this.spaceId, id, "md").catch(() => null),
//         ),
//       );

//       return newObjects.filter((obj): obj is ObjectWithBody => obj !== null)
//     }),
//     catchError((err) => {
//         this.logger.error(`Failed to poll objects in space ${this.spaceId}:`, err);
//         return of([]);
//       }),
//   );
// }

// private getMention(): Observable<ObjectWithBody[]>  {
//   const oneMinute = 60 * 1000;

//   return timer(0, oneMinute).pipe(
//     switchMap(async() => {
//       const objects = await this.anytype.searchObjects(this.spaceId, { query: this.botName });

//       // TODO: improve by first fetching all types of objects and then filtering by notAllowedObjTypes
//       const candidates = objects.filter(
//         (obj) =>
//           !obj.archived &&
//           obj.layout !== "chat" &&
//           obj.type_key !== "chat" &&
//           !obj.name?.toLowerCase().startsWith("chat with "),
//       );

//       const objectsMarkdown = await Promise.all(
//         candidates.map((obj) => {
//           return this.anytype.getObject(this.spaceId, obj.id, "md").catch((err) => {
//             this.logger.warn(`Failed to fetch body for object ${obj.id}: ${err.message}`);
//             return null;
//           })
//         }),
//       );

//       const mentions = objectsMarkdown.filter(
//         (obj): obj is ObjectWithBody => obj !== null && this.isMention(obj),
//       );

//       if (mentions.length > 0) {
//         this.logger.debug(`Found ${mentions.length} active mention(s)`);
//         console.log('mentions.length', mentions.length)
//         console.log('mentions', mentions)
//       }

//       return mentions;
//     }),
//     catchError((err) => {
//         this.logger.error(`Failed to poll objects in space ${this.spaceId}:`, err);
//         return of([]);
//       }),
//   );
// }

// private getMention(): Observable<AnytypeObject[]>  {
//   const oneMinute = 60 * 1000;

//   return timer(0, oneMinute).pipe(
//     switchMap(() =>
//       from(this.anytype.searchObjects(this.spaceId, { query: this.botName })).pipe(
//         // TODo - get all available in space objects and then filter them against allowed list
//         map((objects) =>
//           objects.filter(
//             (obj) =>
//               !obj.archived &&
//               obj.layout !== "chat" &&
//               obj.type_key !== "chat" &&
//               !obj.name?.toLowerCase().startsWith("chat with "),
//           )),
//         concatMap((obj) =>
//           from(this.anytype.getObject(this.spaceId, obj.id, "md")).pipe(
//             catchError((err) => {
//               this.logger.warn(`Failed to fetch body for object ${obj.id}:`, err);
//               return of(null);
//             }),
//           ),
//         ),
//         filter((fullObj): fullObj is ObjectWithBody => {
//           if (!fullObj) return false;
//           return this.isMention(fullObj);
//         }),
//         tap((filtered) => console.log(`Filtered objects: ${JSON.stringify(filtered, null, 2)}`)),
//         catchError((err) => {
//           this.logger.error(`Failed to poll objects in space ${this.spaceId}:`, err);
//           return EMPTY;;
//         })
//       )
//     ),
//   );
// }

import { createHash } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import {
  catchError,
  exhaustMap,
  filter,
  firstValueFrom,
  from,
  map,
  mergeMap,
  type Observable,
  of,
  switchMap,
  takeUntil,
  timer,
  toArray,
} from "rxjs";
import type { AppConfig } from "../app.config";
import { AnytypeService, type ObjectWithBody } from "../client";
import { LLM_SERVICE } from "../llm/llm.module";
import { AbstractLlmService, LlmResponse } from "../llm/types";
import { AbstractObserver, type ObserverFactory } from "./types";

interface MentionTask {
  readonly mentionKey: string;
  readonly objectId: string;
  readonly responsePageId: string;
  readonly prompt: string;
  readonly objectName: string;
}

@Injectable()
export class ObjectObserverFactory implements ObserverFactory {
  constructor(
    private readonly anytype: AnytypeService,
    @Inject(LLM_SERVICE) private readonly llm: AbstractLlmService,
    private readonly config: ConfigService<AppConfig, true>,
  ) {}

  create(spaceId: string, botName: string): ObjectObserver {
    return new ObjectObserver(
      spaceId,
      botName,
      this.anytype,
      this.llm,
      this.config.get("OBSERVER_DEBOUNCE_MS"),
      this.config.get("OBSERVER_RETRY_DELAY_MS"),
    );
  }
}

export class ObjectObserver extends AbstractObserver {
  private readonly handledMentions = new Map<string, string>();
  private readonly lastModifiedDates = new Map<string, string>();

  constructor(
    private readonly spaceId: string,
    private readonly botName: string,
    private readonly anytype: AnytypeService,
    private readonly llm: AbstractLlmService,
    private readonly debounceMs: number,
    private readonly retryDelayMs: number,
  ) {
    super();
  }

  run(): Observable<unknown> {
    const pollInterval = 60 * 1000;
    const MAX_CONCURRENT_LLM_REQUESTS = 3;

    return timer(0, pollInterval).pipe(
      exhaustMap(() => from(this.scanAndPrepareTasks())),
      mergeMap((tasks) => from(tasks)),
      mergeMap(
        (task) =>
          from(this.generateAndFillAnswer(task)).pipe(
            catchError((err) => {
              this.logger.error(`Task execution failed: ${err.message}`);
              return of(null);
            }),
          ),
        MAX_CONCURRENT_LLM_REQUESTS,
      ),
      takeUntil(this.destroy$),
    );
  }

  private getBotBacklinkIds(): Observable<string[]> {
    return from(this.anytype.getObject(this.spaceId, this.getBotId())).pipe(
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
        );
      }),
    );
  }

  private scanAndPrepareTasks(): Observable<MentionTask[]> {
    return this.getObjectsWithMentions().pipe(
      map((objects) => {
        const tasksToRun: MentionTask[] = [];
        for (const object of objects) {
          if (!object?.markdown) continue;

          const tasks = this.handleObject(object);

          if (tasks.length > 0) {
            tasksToRun.push(...tasks);
          }
        }
        return tasksToRun;
      }),
      catchError((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        this.logger.error(`Failed to scan and prepare tasks: ${msg}`);
        return of([]);
      }),
    );
  }

  private async handleObject(object: ObjectWithBody): Promise<MentionTask[]> {
    const lastModified = object.properties?.find((p) => p.key === "last_modified_date")?.date ?? "";

    // Guard: object has not changed since last check
    if (this.lastModifiedDates.get(object.id) === lastModified) {
      return [];
    }

    if (!object.markdown) {
      return [];
    }

    const lines = object.markdown.split("\n");
    const mentionPattern = new RegExp(
      `\\[${this.botName}\\]\\(anytype:\\/\\/object\\?objectId=_participant_[^)]+\\)`,
    );

    const occurrences = new Map<string, number>();
    const tasksToRun: MentionTask[] = [];
    let objectModified = false;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!mentionPattern.test(line)) continue;

      const cleanPrompt = this.extractCleanPrompt(line);
      const count = occurrences.get(cleanPrompt) ?? 0;
      occurrences.set(cleanPrompt, count + 1);

      const mentionKey = this.buildMentionKey(object.id, cleanPrompt, count);

      // Guard: already handled or currently in progress
      if (this.handledMentions.has(mentionKey)) continue;

      // Cold start / hydration: recognize existing response link to avoid re-generating on restart
      if (lines[i + 1]?.includes("↳ [Agent Response:")) {
        this.handledMentions.set(mentionKey, "already_linked");
        continue;
      }

      this.logger.log(`⚡ Fresh mention detected: "${cleanPrompt}" (Key: ${mentionKey})`);

      // 1. Immediately reserve key in map to prevent duplicate tasks on subsequent poll ticks
      this.handledMentions.set(mentionKey, "pending");

      // 2. Create placeholder page
      const placeholder = await this.anytype.createObject(this.spaceId, {
        type_key: "page",
        name: `Agent Response: ${object.name || "Task"}`,
        body: `# ⏳ В обработке...\n\nАгент "${this.botName}" генерирует ответ на запрос:\n>*${cleanPrompt}*`,
      });
      this.handledMentions.set(mentionKey, placeholder.id);

      // 3. Insert response link directly under the mention line
      const responseLink = `> ↳ [Agent Response: ${object.name || "Task"}](anytype://object?objectId=${placeholder.id}&spaceId=${this.spaceId})`;
      lines[i] = `${line}\n${responseLink}`;
      objectModified = true;

      tasksToRun.push({
        mentionKey,
        objectId: object.id,
        responsePageId: placeholder.id,
        prompt: cleanPrompt,
        objectName: object.name || "Task",
      });
    }

    if (objectModified) {
      const updatedObject = await this.anytype.updateObject(this.spaceId, object.id, {
        markdown: lines.join("\n"),
      });
      const freshDate = updatedObject.properties?.find((p) => p.key === "last_modified_date")?.date;
      this.lastModifiedDates.set(object.id, freshDate ?? lastModified);
    } else this.lastModifiedDates.set(object.id, lastModified);

    return tasksToRun;
  }

  private async generateAndFillAnswer(task: MentionTask): Promise<void> {
    this.logger.log(`🧠 Generating LLM response for: "${task.prompt}"...`);

    try {
      const event = await firstValueFrom(
        this.llm
          .run(this.spaceId, {
            type: "object_mention",
            objectId: task.objectId,
            objectName: task.objectName,
            task: task.prompt,
          })
          .pipe(
            filter((e): e is LlmResponse => e instanceof LlmResponse),
            catchError((err) => {
              this.logger.error(`LLM call failed: ${err.message}`);
              return of(null);
            }),
          ),
      );

      const finalMarkdown = event?.text?.trim() || "Ответ не был сгенерирован.";

      await this.anytype.updateObject(this.spaceId, task.responsePageId, {
        name: `Agent Response: ${task.objectName}`,
        markdown: `# Agent Response\n\n${finalMarkdown}`,
      });

      this.logger.log(`✅ Completed response: ${task.mentionKey}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.error(`Failed to update response page ${task.responsePageId}: ${msg}`);

      await this.anytype
        .updateObject(this.spaceId, task.responsePageId, {
          name: `Agent Response: ⚠️ Ошибка`,
          markdown: `Не удалось сгенерировать ответ: ${msg}`,
        })
        .catch(() => null);
    }
  }

  // temp hardcode
  private getBotId(): string {
    return "_participant_bafyreiatodrzle2rm4qyj4556xykozuqy327valyvxmwhkxotfofhrsctu_12orhxm981fr7_A7eruXe9x7QoWQzHes6brEL5nxjkxCcWbu9y2fAfVi9VTE2c";
  }

  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  private extractCleanPrompt(line: string): string {
    const botMentionRegex = new RegExp(
      `\\[${this.escapeRegex(this.botName)}\\]\\(anytype:\\/\\/object\\?objectId=[^)]+\\)`,
      "g",
    );
    return line.replace(botMentionRegex, "").trim();
  }

  private buildMentionKey(objectId: string, prompt: string, occurrenceIndex: number): string {
    const hash = createHash("sha256")
      .update(`${prompt}:${occurrenceIndex}`)
      .digest("hex")
      .slice(0, 16);
    return `${objectId}:${hash}`;
  }
}
