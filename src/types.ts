import { ConsumerConfig, IHeaders, InstrumentationEvent } from 'kafkajs';
import { Subject } from 'rxjs';

export type ConsumeParams = {
  config: ConsumerConfig;
  topic: string;
  partition?: number;
  offset?: string;
  recoverOffsets?: boolean;
};
export type Message = {
  partition: number;
  offset: string;
  headers: IHeaders;
  value: string;
  workComplete: Subject<void>;
};
export type Event = {
  type: string;
  payload?: InstrumentationEvent<any>;
}