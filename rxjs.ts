import { filter, map, Observable, of, switchMap, timer } from "rxjs";

// Helper sleep function
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// ШАГ 1: Одно значение vs Поток значений
// Задача: Передать данные потребителю.
// ============================================================================

// 1.1 Promise: умеет отдавать только ОДНО значение
export async function step1_Promise() {
  console.log("--- ШАГ 1: Promise (Одно значение) ---");
  const myPromise = new Promise<string>((resolve) => {
    resolve("Значение 1");
    resolve("Значение 2"); // Бесполезно: промис уже завершен
  });

  const res = await myPromise;
  console.log("Promise выдал:", res);
}

// 1.2 RxJS Observable: может отдавать МНОГО значений по очереди
export function step1_RxJS() {
  console.log("\n--- ШАГ 1: RxJS (Поток значений) ---");
  const myObservable$ = new Observable<string>((subscriber) => {
    subscriber.next("Значение 1");
    subscriber.next("Значение 2");
    subscriber.next("Значение 3");
    subscriber.complete();
  });

  myObservable$.subscribe((val) => console.log("RxJS выдал:", val));
}

// ============================================================================
// ШАГ 2: Отмена асинхронной операции (Cancellation)
// Задача: Запустить таймер на 500мс, но передумать через 150мс и отменить.
// ============================================================================

// 2.1 Promise: Отмена через AbortController
export async function step2_Promise() {
  console.log("\n--- ШАГ 2: Promise (Отмена через AbortController) ---");
  const controller = new AbortController();

  const promiseTask = new Promise<string>((resolve, reject) => {
    const timerId = setTimeout(() => resolve("Данные загружены!"), 500);

    // Слушаем сигнал отмены
    controller.signal.addEventListener("abort", () => {
      clearTimeout(timerId);
      reject(new DOMException("Aborted", "AbortError"));
    });
  });

  // Отменяем через 150мс
  setTimeout(() => controller.abort(), 150);

  try {
    const result = await promiseTask;
    console.log("Promise результат:", result);
  } catch (err: any) {
    if (err.name === "AbortError") console.log("  ❌ Promise: Запрос успешно отменен!");
  }
}

// 2.2 RxJS: Отмена через .unsubscribe()
export async function step2_RxJS() {
  console.log("\n--- ШАГ 2: RxJS (Отмена через unsubscribe) ---");
  const observableTask$ = new Observable<string>((subscriber) => {
    const timerId = setTimeout(() => {
      subscriber.next("Данные загружены!");
      subscriber.complete();
    }, 500);

    // Teardown: функция очистки при отписке
    return () => {
      clearTimeout(timerId);
      console.log("  ❌ RxJS: Отписка сработала, таймер очищен!");
    };
  });

  console.log("HELOO", observableTask$);

  const subscription = observableTask$.subscribe((val) => console.log("RxJS результат:", val));

  // Отписываемся через 150мс
  await sleep(150);
  subscription.unsubscribe();
}

// ============================================================================
// ШАГ 3: Трансформация данных (map)
// Задача: Получить число 10 и умножить его на 2.
// ============================================================================

// 3.1 Promise: Трансформация через .then() или async/await
export async function step3_Promise() {
  console.log("\n--- ШАГ 3: Promise (Трансформация через .then) ---");
  const getNumber = () => Promise.resolve(10);

  const doubled = await getNumber().then((x) => x * 2);
  console.log("Promise map результат:", doubled);
}

// 3.2 RxJS: Трансформация через pipe(map(...))
export function step3_RxJS() {
  console.log("\n--- ШАГ 3: RxJS (Трансформация через pipe + map) ---");
  of(10)
    .pipe(map((x) => x * 2))
    .subscribe((doubled) => console.log("RxJS map результат:", doubled));
}

// ============================================================================
// ШАГ 4: Фильтрация данных (filter)
// Задача: Получить значение и обработать его ТОЛЬКО если оно больше 10.
// ============================================================================

// 4.1 Promise: Фильтрация через if-guard в async функции
export async function step4_Promise() {
  console.log("\n--- ШАГ 4: Promise (Фильтрация через if) ---");
  async function processNumber(num: number) {
    if (num <= 10) return; // Игнорируем числа <= 10
    console.log("Promise filter результат:", num);
  }

  await processNumber(5); // Будет проигнорировано
  await processNumber(25); // Будет обработано
}

// 4.2 RxJS: Фильтрация через pipe(filter(...))
export function step4_RxJS() {
  console.log("\n--- ШАГ 4: RxJS (Фильтрация через pipe + filter) ---");
  of(5, 25)
    .pipe(filter((x) => x > 10))
    .subscribe((num) => console.log("RxJS filter результат:", num));
}

// ============================================================================
// ШАГ 5: Смена асинхронного запроса с отменой старого (switchMap)
// Задача: Быстро запросить User 1, затем User 2. Запрос User 1 должен отмениться.
// ============================================================================

// 5.1 Promise: Ручной менеджмент AbortController
export async function step5_Promise() {
  console.log("\n--- ШАГ 5: Promise (Ручной AbortController при новом запросе) ---");
  let activeController: AbortController | null = null;

  async function fetchUser(userId: number, delayMs: number) {
    // 1. Если был старый незавершенный запрос — отменяем его
    if (activeController) activeController.abort();

    const controller = new AbortController();
    activeController = controller;

    try {
      console.log(`[Promise] Запрос за User ${userId} отправлен...`);
      await sleep(delayMs);
      if (controller.signal.aborted) return;
      console.log(`  ✅ [Promise] Ответ получен: User ${userId}`);
    } catch (err) {
      // Игнорируем отмену
    }
  }

  fetchUser(1, 300); // Медленный запрос
  await sleep(50);
  await fetchUser(2, 100); // Быстрый запрос (отменяет User 1)
}

// 5.2 RxJS: Автоматическая отмена через switchMap
export async function step5_RxJS() {
  console.log("\n--- ШАГ 5: RxJS (Авто-отмена через switchMap) ---");
  const userClicks$ = new Observable<number>((subscriber) => {
    subscriber.next(1);
    setTimeout(() => subscriber.next(2), 50);
    setTimeout(() => subscriber.complete(), 350);
  });

  return new Promise<void>((resolve) => {
    userClicks$
      .pipe(
        switchMap((userId) => {
          console.log(`[RxJS] Запрос за User ${userId} отправлен...`);
          const delayMs = userId === 1 ? 300 : 100;
          return timer(delayMs).pipe(map(() => `User ${userId}`));
        }),
      )
      .subscribe({
        next: (res) => console.log(`  ✅ [RxJS] Ответ получен: ${res}`),
        complete: () => resolve(),
      });
  });
}

// ============================================================================
// Главный запуск всех шагов
// ============================================================================
async function main() {
  await step1_Promise();
  step1_RxJS();

  await step2_Promise();
  await step2_RxJS();

  await step3_Promise();
  step3_RxJS();

  await step4_Promise();
  step4_RxJS();

  await step5_Promise();
  await step5_RxJS();
}

main().catch(console.error);
