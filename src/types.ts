import { ConsumerConfig, ConsumerRunConfig, IHeaders, InstrumentationEvent } from 'kafkajs';
import { Subject } from 'rxjs';

export type ConsumeParams = {
  config: ConsumerConfig;
  topic?: string;
  topics?: string[];
  partition?: number;
  offset?: string;
  recoverOffsets?: boolean;
  runConfig?: RunConfig;
};
export type Message = {
  partition: number;
  offset: string;
  headers: IHeaders;
  value: string;
  workComplete: () => void;
};
export type Event = {
  type: string;
  payload?: InstrumentationEvent<any>;
};
export type RunConfig = Pick<
  ConsumerRunConfig,
  'autoCommit' | 'autoCommitInterval' | 'autoCommitThreshold' | 'partitionsConsumedConcurrently'
>;
