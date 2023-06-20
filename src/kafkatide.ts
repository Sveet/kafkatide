import {
  CompressionTypes,
  Kafka,
  KafkaConfig,
  Message as KafkaMessage,
  ProducerConfig,
  RecordMetadata,
  logLevel,
} from 'kafkajs';
import {
  asyncScheduler,
  buffer,
  bufferTime,
  firstValueFrom,
  from,
  mergeWith,
  Observable,
  observeOn,
  scheduled,
  share,
  Subject,
  Subscriber,
  take,
  takeUntil,
} from 'rxjs';
import { Event, Message, ConsumeParams } from './types';
import { getOffsetHandlers } from './offsets';
import { waitFor } from './operators';

export default class KafkaTide {
  private kafka: Kafka;
  constructor(kafkaConfig?: KafkaConfig) {
    kafkaConfig.logLevel ??= logLevel.WARN;
    this.kafka = new Kafka(kafkaConfig);
  }

  /**
   * @param topic - Kafka topic to produce to
   * @param producerConfig - Optional KafkaJS producer config
   * @returns Object containing:
   *  - sendSubject: RxJS Subject to send messages to Kafka
   *  - event$: Observable of KafkaJS producer events
   *  - error$: Observable of KafkaJS producer errors
   *  - disconnectSubject: RxJS Subject to disconnect the producer
   */
  produce = (topic: string, producerConfig?: ProducerConfig) => {
    let producer = this.kafka.producer(producerConfig);
    const send = async (
      topic: string,
      messages: KafkaMessage[],
      retries = 1,
    ): Promise<RecordMetadata[]> => {
      try {
        const response = await producer.send({
          topic: topic,
          messages: messages,
          compression: CompressionTypes.GZIP,
        });
        return response;
      } catch (err: any) {
        if (retries > 0 && `${err}`.includes('The producer is disconnected')) {
          console.warn(
            `Sending KafkaJS messages failed because Producer was disconnected. Reconnecting (${retries}) and retrying to send.`,
          );
          producer = this.kafka.producer(producerConfig);
          await producer.connect();
          return send(topic, messages, retries - 1);
        } else {
          throw err;
        }
      }
    };
    const connect$ = from(producer.connect()).pipe(share());
    const disconnectSubject = new Subject<void>();
    const sendSubject = new Subject<KafkaMessage>();
    const send$ = sendSubject.asObservable().pipe(share());
    const errorSubject = new Subject<Error>();
    const error$ = errorSubject.asObservable();
    const event$ = new Observable<Event>((subscriber) => {
      for (const event of Object.values(producer.events)) {
        producer.on(event, (e) => {
          subscriber.next({
            type: event,
            payload: e,
          });
        });
      }
    });
    const buffered$ = send$.pipe(buffer(connect$), take(1));
    scheduled(send$, asyncScheduler)
      .pipe(waitFor(connect$), bufferTime(250), mergeWith(buffered$), takeUntil(disconnectSubject))
      .subscribe({
        next: async (records) => {
          if (records.length <= 0) return;

          return send(topic, records).catch((err) => {
            if (`${err}`.toLowerCase().includes('disconnected')) {
              sendSubject.error(err);
            }
            errorSubject.next(err);
          });
        },
        error: () => {
          producer.disconnect();
        },
        complete: () => {
          producer.disconnect();
        },
      });
    return { sendSubject, event$, error$, disconnectSubject };
  };

  /**
   * @param config - KafkaJS consumer config
   * @param topic - Kafka topic to consume from
   * @param partition - Optional partition to consume from
   * @param offset - Optional offset to start consuming from
   * @param runConfig - Optional config to pass to consumer.run
   * @returns Object containing:
   *  - message$: Observable of consumed Kafka messages
   *  - event$: Observable of KafkaJS consumer events
   */
  consume = ({ config, topic, partition, offset, runConfig }: ConsumeParams) => {
    runConfig ??= {};
    runConfig.autoCommit ??= true;
    const { startWorkingOffset, finishWorkingOffset } = getOffsetHandlers();
    const consumer = this.kafka.consumer(config);
    const run = async (subscriber: Subscriber<Message>) => {
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: false });

      consumer.run({
        ...runConfig,
        eachBatchAutoResolve: false,
        eachBatch: async ({
          batch,
          isRunning,
          isStale,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
        }) => {
          const promises = batch.messages.map(async (m) => {
            const workComplete = new Subject<void>();
            subscriber.next({
              partition: batch.partition,
              offset: m.offset,
              headers: m.headers,
              value: m.value.toString(),
              workComplete,
            });
            await heartbeat();
            if (!runConfig.autoCommit) {
              startWorkingOffset(batch.partition, Number.parseInt(m.offset));
            }
            await firstValueFrom(workComplete);
            if (!isRunning() || isStale()) return;

            if (runConfig.autoCommit) {
              await commitOffsetsIfNecessary();
            } else {
              const offsetToCommit = finishWorkingOffset(
                batch.partition,
                Number.parseInt(m.offset),
              );
              if (offsetToCommit) {
                await commitOffsetsIfNecessary({
                  topics: [
                    { topic, partitions: [{ partition: batch.partition, offset: m.offset }] },
                  ],
                });
              }
            }
            resolveOffset(m.offset);
          });
          const results = await Promise.allSettled(promises);
          const errors = results
            .filter((r) => r.status == 'rejected')
            .map((r) => (r as PromiseRejectedResult).reason);
          if (errors?.length > 0) {
            subscriber.error({ message: 'Error committing offsets', errors });
          }
        },
      });

      if (partition !== undefined && offset !== undefined) {
        console.debug(`${config.groupId} seeking offset: ${offset}, partition: ${partition}`);
        consumer.seek({ topic, partition, offset });
      }
    };

    const restartConsumer = async (subscriber: Subscriber<Message>) => {
      try {
        await consumer.disconnect();
        await run(subscriber);
      } catch (err) {
        console.error(`Failed to restart consumer ${config.groupId}: ${err}`);
      }
    };
    const message$ = new Observable<Message>((subscriber) => {
      consumer.on('consumer.crash', (e) => {
        const eventString = `${typeof e.payload.error} ${e.payload.error} ${e.payload.error.stack}`;
        if (e.payload.restart) {
          // rebalancing sometimes runs out of internal retries and requires a consumer restart
          console.error(
            `Consumer ${config.groupId} received a non-retriable error: ${eventString}`,
          );
          return restartConsumer(subscriber);
        }
      });
      run(subscriber).catch((err) => {
        console.error(`KafkaJS consumer.run threw error ${err}`);
        return restartConsumer(subscriber);
      });

      return () => {
        console.debug(`Running clean-up for consumer ${config.groupId}`);
        consumer
          .disconnect()
          .then(() => console.debug(`Disconnected consumer ${config.groupId}`))
          .catch((err) => console.error(`Error disconnecting consumer ${config.groupId} ${err}`));
      };
    }).pipe(observeOn(asyncScheduler));
    const event$ = new Observable<Event>((subscriber) => {
      for (const event of Object.values(consumer.events)) {
        consumer.on(event, (e) => {
          subscriber.next({
            type: event,
            payload: e,
          });
        });
      }
    });
    return { message$, event$ };
  };
}
