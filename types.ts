import { ConsumerConfig, IHeaders, InstrumentationEvent } from "kafkajs";

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
  workComplete: () => Promise<void>;
};
export type EventOutput = {
  event: string;
  payload?: InstrumentationEvent<any>;
}