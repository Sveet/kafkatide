import { ConsumerConfig, IHeaders } from "kafkajs";

export type GetConsumerMessagesParams = {
  config: ConsumerConfig;
  topic: string;
  partition?: number;
  offset?: string;
  recoverOffsets?: boolean;
};
export type GetConsumerMessagesOutput = ConsumerMessageOutput | ConsumerEventOutput;
export type ConsumerMessageOutput = {
  type: 'message';
  headers: IHeaders;
  body: string;
  workComplete: () => Promise<void>;
};
export type ConsumerEventOutput = {
  type: 'event';
  body: string;
};