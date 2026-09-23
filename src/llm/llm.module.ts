import { Module } from "@nestjs/common";
import { ClientModule } from "../client";
import { HostModelService } from "./host.model";
import { AbstractLlmService } from "./types";

export const LLM_SERVICE = Symbol.for("LLM_SERVICE");

@Module({
  imports: [ClientModule],
  providers: [
    HostModelService,
    // ApiModelService,
    {
      provide: LLM_SERVICE,
      inject: [
        HostModelService,
        // ApiModelService
      ],
      useFactory: async (
        hostModel: HostModelService,
        // apiModel: ApiModelService,
      ): Promise<AbstractLlmService> => {
        // TODO: pick providers from config.LLM (cli/api) and wrap them in a fallback chain.
        const service: AbstractLlmService = hostModel;

        // init() inside DI factory: invalid config/SSH fails immediately during module bootstrap
        await service.init();
        return service;
      },
    },
  ],
  exports: [LLM_SERVICE],
})
export class LlmModule {}
