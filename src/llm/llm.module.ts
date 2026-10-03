import { Module } from "@nestjs/common";
import { ClientModule } from "../client";
import { AnytypeProxy } from "../client/anytype.proxy";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import type { SshClient } from "../ssh/ssh.client";
import { SshModule } from "../ssh/ssh.module";
import { SshService } from "../ssh/ssh.service";
import { AgyCliProvider } from "./cli/agy";
import { LLM_PROVIDERS } from "./llm.constants";
import { LlmService } from "./llm.service";

@Module({
  imports: [ClientModule, SshModule],
  providers: [
    {
      provide: LLM_PROVIDERS,
      inject: [APP_CONFIG, SshService, AnytypeProxy],
      useFactory: (config: AppConfig, ssh: SshService, proxy: AnytypeProxy) => {
        return Promise.allSettled(
          config.LLM.map(async (provider) => {
            if (provider.MODE === "cli") {
              await proxy.start();

              // TODO: add a local process spawner here later
              let spawner: SshClient | any;

              if (provider.SSH) {
                spawner = await ssh.connect(provider.SSH, provider.SSH_KEY, {
                  localPort: proxy.port,
                });
              }

              if (provider.CLI.endsWith("agy")) {
                return new AgyCliProvider(spawner, provider.CLI);
              }
            }
          }),
          // TODO: add a logger here to track the results
        ).then((results) => results.filter((r) => r.status === "fulfilled").map((r) => r.value));
      },
    },
    LlmService,
  ],
  exports: [LlmService],
})
export class LlmModule {}
