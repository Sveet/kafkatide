import { ConsumerConfig, IHeaders, InstrumentationEvent } from "kafkajs";
import { Subject } from "rxjs";

export type ConsumeParams = {
  config: ConsumerConfig;
  topic: string;
  partition?: number;
  offset?: string;
  recoverOffsets?: boolean;
};
export type ConsumerMessageOutput = {
  type: 'message';
  headers: IHeaders;
  body: string;
  workComplete: Subject<void>;
};
export type EventOutput = {
  event: string;
  payload?: InstrumentationEvent<any>;
}