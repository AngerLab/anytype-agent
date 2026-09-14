import { Module } from "@nestjs/common";
import { ClientModule } from "../client";
import { LlmModule } from "../llm/llm.module";
import { ChatsObserverFactory } from "./chats.observer";
import { OBSERVERS } from "./constants";
import { ObjectObserverFactory } from "./object.observer";
import { ObserverService } from "./observer.service";

@Module({
  imports: [ClientModule, LlmModule],
  providers: [
    ChatsObserverFactory,
    ObjectObserverFactory,
    {
      provide: OBSERVERS,
      inject: [ChatsObserverFactory, ObjectObserverFactory],
      useFactory: (...args) => args,
    },
    ObserverService,
  ],
  exports: [ObserverService],
})
export class ObserverModule {}
