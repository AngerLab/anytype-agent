import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { NestFactory } from "@nestjs/core";
import { AnytypeService } from "../client";
import { makeAppEnv } from "../config/__tests__/fixtures";
import { APP_CONFIG } from "../config/config.module";
import { LLM_SERVICE } from "../llm/llm.module";
import { AbstractLlmService } from "../llm/types";

/**
 * Boot smoke test: `tsc` cannot verify Nest's DI wiring (injection is resolved at runtime
 * from `@Inject`/`emitDecoratorMetadata`), so this is the only guard that the provider
 * graph actually composes and the app starts.
 */
describe("AppModule boots (DI wiring)", () => {
  let fetchSpy: ReturnType<typeof spyOn>;
  let initSpy: ReturnType<typeof spyOn>;
  const originalEnv = { ...process.env };

  beforeAll(async () => {
    Object.assign(
      process.env,
      makeAppEnv({ "ANYTYPE.BOT_NAME": "NestBot", "ANYTYPE.API_KEY": "nest_key_999" }),
    );
    // Bootstrap reaches the network (Anytype healthcheck) and the host (SSH `--help`).
    fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ data: [] }), { status: 200 }),
    );
    const { HostModelService } = await import("../llm/host.model");
    initSpy = spyOn(HostModelService.prototype, "init").mockResolvedValue();
  });

  afterAll(() => {
    fetchSpy?.mockRestore();
    initSpy?.mockRestore();
    process.env = originalEnv;
  });

  it("resolves the full DI graph and runs the LLM init at bootstrap", async () => {
    const { AppModule } = await import("../app.module");
    const app = await NestFactory.createApplicationContext(AppModule, { logger: false });

    expect(app.get(APP_CONFIG)).toBeDefined();
    expect(app.get(AnytypeService)).toBeInstanceOf(AnytypeService);
    expect(app.get(LLM_SERVICE)).toBeInstanceOf(AbstractLlmService);
    // The LLM DI factory must call init() so bad config/SSH fails at bootstrap, not mid-job.
    expect(initSpy).toHaveBeenCalledTimes(1);

    await app.close();
  });
});
